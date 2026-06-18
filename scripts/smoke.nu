# Shared end-to-end smoke test — runs against ANY catapulte base URL, native or
# Cloudflare. Mirrors the upstream scripts/smoke-test.sh assertion (submit an
# email, confirm it actually delivers) but over the HTTP API only, so the exact
# same test works whether catapulte runs on a box or on Workers:
#
#   nu scripts/smoke.nu http://localhost:3000              # native / compose
#   nu scripts/smoke.nu https://<worker>.workers.dev       # cloudflare
#
# Delivery is confirmed via GET /emails/{id}/events (delivery.succeeded), which
# both runtimes expose identically — no peeking at a mail sink required.
# Exits non-zero on failure (CI-friendly). Sender defaults to a verified-domain
# placeholder; override with CATAPULTE_SMOKE_SENDER for a real send.

# Config from env, else fnox, else a fallback — so once you've onboarded a
# domain and run `fnox set -p keychain CATAPULTE_SMOKE_SENDER you@mail.you.com`,
# `mise run test:cf` goes green with no flags.
def cfg [name: string, fallback: string] {
  let e = ($env | get -o $name | default "" | str trim)
  if ($e | is-not-empty) { return $e }
  # `complete` captures the exit code and never raises, so an unset fnox key
  # just falls through to the fallback instead of failing the run.
  let r = (^fnox get $name | complete)
  let f = (if $r.exit_code == 0 { $r.stdout | str trim } else { "" })
  if ($f | is-not-empty) { $f } else { $fallback }
}

def main [base: string] {
  let sender = (cfg "CATAPULTE_SMOKE_SENDER" "smoke-test@example.com")
  let recipient = (cfg "CATAPULTE_SMOKE_RECIPIENT" "recipient@example.com")
  # Run under a throwaway tenant so the test is fully isolated and self-cleaning
  # — its data lives in its own DO and never touches a real tenant.
  let tenant = $"smoke-(random uuid)"
  let hdr = ["X-Catapulte-Tenant" $tenant]
  print $"smoke: ($base)  sender=($sender)  tenant=($tenant)"

  # 1. wait for readiness
  mut ready = false
  for _ in 1..30 {
    let ok = (try { (http get --full $"($base)/health/ready").status == 200 } catch { false })
    if $ok { $ready = true; break }
    sleep 1sec
  }
  if not $ready { print "  FAIL: /health/ready never came up"; exit 1 }
  print "  health/ready: ok"

  # 2. submit
  let body = {
    sender: $sender,
    recipients: [{kind: "to", address: $recipient}],
    subject: "Smoke Test",
    body: {kind: "plain", text: "Hello from the smoke test"}
  }
  let id = (http post --content-type application/json --headers $hdr $"($base)/emails" $body | get id)
  print $"  submitted: ($id)"

  # 3. confirm delivery via the lifecycle events
  mut outcome = "timeout"
  for _ in 1..30 {
    let types = (try { http get --headers $hdr $"($base)/emails/($id)/events" | get events | get event_type } catch { [] })
    if ("delivery.succeeded" in $types) { $outcome = "succeeded"; break }
    if ("delivery.failed" in $types) { $outcome = "failed"; break }
    sleep 1sec
  }

  if $outcome == "succeeded" {
    print "  SMOKE PASS: email delivered"
  } else if $outcome == "failed" {
    let reason = (try {
      http get --headers $hdr $"($base)/emails/($id)/events" | get events
      | where event_type == "delivery.failed" | get 0.payload.reason
    } catch { "unknown" })
    print $"  SMOKE FAIL: delivery.failed — ($reason)"
    exit 1
  } else {
    print "  SMOKE FAIL: no terminal delivery event within timeout"
    exit 1
  }
}
