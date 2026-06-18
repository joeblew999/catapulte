# Onboard / manage Cloudflare Email Sending subdomains via the CF API.
# Run under `fnox exec` so CLOUDFLARE_API_TOKEN + CATAPULTE_MAIL_ZONE are in env.
#
#   mise run domain:add  acme.mail.yourzone.com   # onboard a sending subdomain
#   mise run domain:list                          # list onboarded subdomains
#   mise run domain:dns  <tag>                     # show its expected DNS records
#   mise run domain:rm   <tag>                     # remove it
#
# CF auto-writes the cf-bounce DKIM/SPF/DMARC records (the zone uses CF DNS), so
# this is genuinely zero-touch. The subdomain must be within CATAPULTE_MAIL_ZONE.

const API = "https://api.cloudflare.com/client/v4"

def token [] { $env.CLOUDFLARE_API_TOKEN? | default "" | str trim }
def hdr [] { ["Authorization" $"Bearer (token)"] }

def zone_id [] {
  let zid = ($env.CATAPULTE_MAIL_ZONE_ID? | default "" | str trim)
  if ($zid | is-not-empty) { return $zid }
  let zname = ($env.CATAPULTE_MAIL_ZONE? | default "" | str trim)
  if ($zname | is-empty) {
    print "set CATAPULTE_MAIL_ZONE (zone name) or CATAPULTE_MAIL_ZONE_ID"
    exit 1
  }
  let r = (http get --headers (hdr) $"($API)/zones?name=($zname)")
  let id = ($r.result | get -o 0.id | default "")
  if ($id | is-empty) { print $"zone not found in account: ($zname)"; exit 1 }
  $id
}

def main [action: string, ...args: string] {
  if ((token) | is-empty) { print "no CLOUDFLARE_API_TOKEN (run via fnox exec / mise)"; exit 1 }
  let zone = (zone_id)
  let base = $"($API)/zones/($zone)/email/sending/subdomains"

  match $action {
    "add" => {
      let name = ($args | get -o 0 | default "")
      if ($name | is-empty) { print "usage: domain:add <subdomain.within.zone>"; exit 1 }
      let r = (http post --content-type application/json --headers (hdr) $base {name: $name})
      if (not $r.success) { print $"failed: ($r.errors)"; exit 1 }
      print $"onboarded ($r.result.name)  tag=($r.result.tag)  dkim=($r.result.dkim_selector)  enabled=($r.result.enabled)"
    }
    "list" => {
      let r = (http get --headers (hdr) $base)
      if (($r.result | length) == 0) { print "no sending subdomains onboarded" } else {
        $r.result | select name tag enabled | print
      }
    }
    "dns" => {
      let tag = ($args | get -o 0 | default "")
      if ($tag | is-empty) { print "usage: domain:dns <tag>"; exit 1 }
      let r = (http get --headers (hdr) $"($base)/($tag)/dns")
      $r.result | print
    }
    "rm" => {
      let tag = ($args | get -o 0 | default "")
      if ($tag | is-empty) { print "usage: domain:rm <tag>"; exit 1 }
      let r = (http delete --headers (hdr) $"($base)/($tag)")
      print $"removed ($tag): success=($r.success)"
    }
    _ => { print $"unknown action ($action) — use add|list|dns|rm"; exit 1 }
  }
}
