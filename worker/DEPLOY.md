# Deploying catapulte to Cloudflare Workers

Everything here runs through `mise` (tasks live in the repo root `mise.toml`)
with Cloudflare creds injected by `fnox` (`CLOUDFLARE_API_TOKEN` /
`CLOUDFLARE_ACCOUNT_ID` in the keychain — see root `fnox.toml`).

The worker is fully self-contained: a SQLite-backed Durable Object is the
storage + queue + scheduler, the real `inbound-http` router is the HTTP surface,
MRML renders the body, and the Email Service binding sends. No D1, no Queues,
no cron.

## Prerequisites (one-time, manual — can't be scripted)

1. **A Cloudflare account** with Workers (paid plan — DO SQLite + Email Service
   require it).
2. **A verified sender domain.** The Email Service `send_email` binding only
   sends from a domain you've verified. In the CF dashboard:
   Email → Email Sending (or Email Routing) → add & verify your domain.
   The `sender` in every request must be at that domain.
3. `fnox` creds set once (shared across all joeblew999 repos):
   ```sh
   fnox set -p keychain CLOUDFLARE_API_TOKEN  <token>
   fnox set -p keychain CLOUDFLARE_ACCOUNT_ID <account-id>
   ```

## One-time bootstrap

```sh
mise run deploy:bootstrap     # creates the R2 attachments bucket
```

(That's just `r2:create` today. The Durable Object and its SQLite class are
provisioned automatically by the first `wrangler deploy` via the
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

**(C) Multi-tenant isolation** — send the `X-Catapulte-Tenant: <id>` header.
Each tenant gets its own Durable Object: isolated SQLite, queue, alarm and
data (verified — one tenant's `GET /emails` never sees another's). No header →
the `default` tenant. This is also how you scale: load spreads across DO
instances. Sender domains (A) are orthogonal — a tenant may use any verified
domain.

## Optional features (env vars)

```toml
[vars]
# Outbound webhooks: POST every lifecycle event (queued/sent/failed) here.
CATAPULTE_WEBHOOK_URL   = "https://hooks.example.com/catapulte"
# CATAPULTE_WEBHOOK_TOKEN sent as `Authorization: Bearer …` (set as a secret).
# Per-host auth for remote MJML templates (host → Authorization header):
CATAPULTE_RESOLVER_AUTH = '{"templates.acme.com":"Bearer xyz"}'
```
- **Webhooks** are best-effort — a down endpoint never blocks email; the event
  is always recorded (`GET /events`).
- **MJML includes**: `<mj-include path="https://…/partial.mjml">` is fetched at
  render time (no binding needed).

## Notes / limits

- **Throughput**: a single DO instance ("default") serializes all work. For
  scale, shard by sender/tenant (change `id_from_name` in `worker/src/lib.rs`).
- **Named templates** are read from the `TEMPLATES` R2 bucket (`<name>.mjml`);
  add `[[r2_buckets]] binding = "TEMPLATES"` and upload templates if you use
  them. **Remote templates** (body `mjml_remote`) need no binding.
