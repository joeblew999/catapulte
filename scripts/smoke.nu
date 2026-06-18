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

def main [base: string] {
  let sender = ($env.CATAPULTE_SMOKE_SENDER? | default "smoke-test@example.com")
  print $"smoke: ($base)  sender=($sender)"

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
    recipients: [{kind: "to", address: "recipient@example.com"}],
    subject: "Smoke Test",
    body: {kind: "plain", text: "Hello from the smoke test"}
  }
  let id = (http post --content-type application/json $"($base)/emails" $body | get id)
  print $"  submitted: ($id)"

  # 3. confirm delivery via the lifecycle events
  mut outcome = "timeout"
  for _ in 1..30 {
    let types = (try { http get $"($base)/emails/($id)/events" | get events | get event_type } catch { [] })
    if ("delivery.succeeded" in $types) { $outcome = "succeeded"; break }
    if ("delivery.failed" in $types) { $outcome = "failed"; break }
    sleep 1sec
  }

  if $outcome == "succeeded" {
    print "  SMOKE PASS: email delivered"
  } else if $outcome == "failed" {
    let reason = (try {
      http get $"($base)/emails/($id)/events" | get events
      | where event_type == "delivery.failed" | get 0.payload.reason
    } catch { "unknown" })
    print $"  SMOKE FAIL: delivery.failed — ($reason)"
    exit 1
  } else {
    print "  SMOKE FAIL: no terminal delivery event within timeout"
    exit 1
  }
}
