# Deploying catapulte to Cloudflare Workers

Everything here runs through `mise` (tasks live in the repo root `mise.toml`)
with Cloudflare creds injected by `fnox` (`CLOUDFLARE_API_TOKEN` /
`CLOUDFLARE_ACCOUNT_ID` in the keychain — see root `fnox.toml`).

The worker is fully self-contained: a SQLite-backed Durable Object is the
storage + queue + scheduler, the real `inbound-http` router is the HTTP surface,
MRML renders the body, and the Email Service binding sends. No D1, no Queues,
no cron.

## Prerequisites (one-time)

1. **A Cloudflare account** with Workers (paid plan — DO SQLite + Email Service
   require it).
2. `fnox` creds set once (shared across all joeblew999 repos):
   ```sh
   fnox set -p keychain CLOUDFLARE_API_TOKEN  <token>
   fnox set -p keychain CLOUDFLARE_ACCOUNT_ID <account-id>
   ```

Real delivery also needs a verified sender domain — but that's now scriptable
too (`mise run domain:scan` / `domain:add`, see [Multi-domain](#multi-domain--multi-tenant-a--b--c)),
not a manual dashboard step.

## One-time bootstrap

```sh
mise run deploy:bootstrap     # creates the R2 buckets (attachments + templates)
```

(That's `r2:create` — both R2 buckets. The Durable Object and its SQLite class
are provisioned automatically by the first `wrangler deploy` via the
`[[migrations]] new_sqlite_classes` entry in `wrangler.toml` — nothing to run.)

Optional — lock the HTTP API behind a bearer token:
```sh
fnox set -p keychain CATAPULTE_HTTP_API_KEY <some-strong-key>
mise run secret:api-key       # pushes it as a Worker secret
```
With it set, every route except `/health/*` requires
`Authorization: Bearer <key>`. Leave it unset for an open API (dev only).

## Deploy

```sh
mise run deploy               # = worker:deploy → wrangler deploy (runs worker-build)
```

wrangler prints the deployed URL (e.g. `https://catapulte-worker.<you>.workers.dev`).

## Verify

```sh
# tell verify where the worker is (once) — either of:
fnox set -p keychain CATAPULTE_WORKER_URL https://catapulte-worker.<you>.workers.dev
export CATAPULTE_WORKER_URL=https://catapulte-worker.<you>.workers.dev

mise run verify
```

To make `mise run test:cf` actually **deliver** (go green), point the smoke at a
sender on a CF-onboarded domain — once, via env or fnox:
```sh
fnox set -p keychain CATAPULTE_SMOKE_SENDER    you@mail.yourdomain.com
fnox set -p keychain CATAPULTE_SMOKE_RECIPIENT inbox@example.com   # optional
mise run test:cf      # now → delivery.succeeded
```
Until a verified sender is set, the smoke uses an unverified placeholder and
correctly reports `delivery.failed` (the test is working — there's just no
domain yet).

`mise run verify` is an end-to-end smoke test (nushell): health, submit, list,
events, **[B]** `/senders` reporting, and **[C]** tenant isolation (posts to a
random tenant and asserts it's invisible to `default`). Prints `verify: PASS`
and exits non-zero on failure, so it's CI-friendly. It hits the open API; if you
set an API key, the protected routes will 401 — unset it for verification.

Watch logs while testing:
```sh
mise run worker:tail
```

## What each binding maps to (wrangler.toml)

| Binding | Purpose |
|---|---|
| `CATAPULTE_STORE` (Durable Object) | storage + queue + alarm scheduler |
| `EMAIL` (send_email) | outbound delivery via the Email Service |
| `ATTACHMENTS` (R2) | attachment blobs |
| `TEMPLATES` (R2) | named MJML templates + `<mj-include>` partials (`<name>.mjml`) |
| `CATAPULTE_HTTP_API_KEY` (secret) | optional bearer auth |

## Routes (the real catapulte API)

- `POST /emails` — send (JSON inline/mjml, or multipart/base64 attachments)
- `POST /emails/batch` — bulk send
- `GET /emails`, `GET /emails/{id}/events`, `GET /events`, `GET /senders`
- `GET /health/live`, `GET /health/ready`

## Multi-domain & multi-tenant (A / B / C)

**(A) Sending from many domains** — no code/config. Verify each domain in your
CF account, then set it as the request's `sender`. The `EMAIL` binding sends
from any verified domain; an unverified one is rejected
(`E_SENDER_DOMAIN_NOT_AVAILABLE`).

**(B) Per-sender quotas + reporting** — set the `CATAPULTE_SENDERS` var (JSON):
```toml
[vars]
CATAPULTE_SENDERS = '[{"name":"primary","match_domain":"acme.com","quota_count":1000,"quota_range":"daily"}]'
```
`GET /senders` reports each sender with live `sent_in_range`/`failed_in_range`.
At delivery the sender is matched by from-domain (a sender with no `match_domain`
is the catch-all); if its quota is exhausted in the window the email is
**deferred** (re-checked, not failed) until the window frees up.
`quota_range` ∈ `hourly|daily|weekly|monthly`. On CF there's one egress, so a
"sender" is a from-domain + quota, not an SMTP relay.

**Sender allowlist (multi-tenant safety)** — each tenant can be restricted to
specific from-addresses/domains so one tenant can't send as another:
```sh
mise run tenant:allow acme mail.acme.test billing@mail.acme.test   # set
mise run tenant:list  acme                                          # show
```
A pattern with `@` = exact address, else a domain. Empty list = unrestricted.
A disallowed `sender` is rejected with 403.

Onboard sending domains/subdomains from the CLI (set `CATAPULTE_MAIL_ZONE` to a
zone you own; CF auto-writes the cf-bounce DKIM/SPF/DMARC):
```sh
mise run domain:add  acme.mail.yourzone.com   # onboard (zero-touch)
mise run domain:list                          # list onboarded subdomains
mise run domain:dns  <tag>                     # show its DNS records
mise run domain:rm   <tag>                     # remove
```
Either model works: one shared domain + per-tenant addresses (simplest), or a
subdomain per tenant for reputation isolation.

**(C) Multi-tenant isolation** — send the `X-Catapulte-Tenant: <id>` header.
Each tenant gets its own Durable Object: isolated SQLite, queue, alarm and
data (verified — one tenant's `GET /emails` never sees another's). No header →
the `default` tenant. This is also how you scale: load spreads across DO
instances. Sender domains (A) are orthogonal — a tenant may use any verified
domain.

## Optional features (env)

Non-secret config goes in `[vars]` (wrangler.toml); anything carrying a
credential must be a **secret** (`wrangler secret put …`) — `[vars]` is
plaintext in the committed config.

```toml
# wrangler.toml [vars] — non-secret:
[vars]
CATAPULTE_WEBHOOK_URL = "https://hooks.example.com/catapulte"   # event fan-out
```
```sh
# secrets (credential-bearing):
wrangler secret put CATAPULTE_WEBHOOK_TOKEN     # sent as Authorization: Bearer …
wrangler secret put CATAPULTE_RESOLVER_AUTH     # {"templates.acme.com":"Bearer xyz"} — per-host remote-template auth
```
- **Webhooks** are best-effort — a down endpoint never blocks email; the event
  is always recorded (`GET /events`).
- **MJML includes** (multi-loader): `<mj-include path="https://…/partial.mjml">`
  is fetched via Fetch; `<mj-include path="header">` loads `header.mjml` from the
  `TEMPLATES` R2 bucket. Upload partials with:
  `wrangler r2 object put catapulte-templates/header.mjml --file header.mjml --remote`

## Notes / limits

- **Throughput / scale**: work is serialized *within* a tenant (its single DO)
  but runs in parallel *across* tenants — each `X-Catapulte-Tenant` is its own
  DO. So you scale by spreading load across tenants; a single hot tenant is the
  only serialization point.
- **Named templates / includes**: the `TEMPLATES` R2 bucket is already bound in
  `wrangler.toml` — just upload partials (`<name>.mjml`). Remote templates
  (`mjml_remote`) and URL `<mj-include>`s need no bucket.
