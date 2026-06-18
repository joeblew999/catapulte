# Manage a tenant's allowed-sender list on the deployed Worker.
#
#   nu scripts/tenant.nu allow <tenant> <pattern>...   # set the allowlist
#   nu scripts/tenant.nu list  <tenant>                # show it
#
# A pattern with `@` matches an exact address; otherwise a domain. An empty
# list = unrestricted. Base URL from CATAPULTE_WORKER_URL or fnox; if the API
# is locked with CATAPULTE_HTTP_API_KEY, that's sent as a bearer token.

def base_url [] {
  let env_url = ($env.CATAPULTE_WORKER_URL? | default "" | str trim)
  if ($env_url | is-not-empty) { return $env_url }
  (do -i { fnox get CATAPULTE_WORKER_URL } | default "" | str trim)
}

def auth_headers [tenant: string] {
  mut h = ["X-Catapulte-Tenant" $tenant]
  let key = ($env.CATAPULTE_HTTP_API_KEY? | default "" | str trim)
  if ($key | is-not-empty) { $h = ($h | append ["Authorization" $"Bearer ($key)"]) }
  $h
}

def main [action: string, tenant: string, ...patterns: string] {
  let base = (base_url)
  if ($base | is-empty) { print "set CATAPULTE_WORKER_URL (env) or fnox set -p keychain CATAPULTE_WORKER_URL https://..."; exit 1 }
  let url = $"($base)/admin/allowed-senders"
  let headers = (auth_headers $tenant)

  match $action {
    "allow" => {
      let result = (http put --content-type application/json --headers $headers $url $patterns)
      print $"tenant ($tenant) allowed senders -> ($result.allowed_senders)"
    }
    "list" => {
      let result = (http get --headers $headers $url)
      print $"tenant ($tenant) allowed senders -> ($result.allowed_senders)"
    }
    _ => { print $"unknown action ($action) — use allow|list"; exit 1 }
  }
}
