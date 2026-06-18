# catapulte — TODO / status

Fork of `jdrouet/catapulte` (Rust MJML/MRML transactional email). Goal: **one
codebase that runs both native (Hetzner/SMTP) and on Cloudflare Workers**, sharing
the domain core. Pick up here.

## Branches

| Branch | What |
|---|---|
| `feat/mise-fnox` | mise + fnox tooling overlay (dev loop) |
| `feat/outbound-cloudflare` | option A — native catapulte sends *via* CF through a Worker shim (issue #722) |
| `feat/worker-runtime` | **option B — catapulte runs entirely on CF Workers** (the main line; everything below lives here) |

Upstream issues filed (offering PRs): catapulte
[#722](https://github.com/jdrouet/catapulte/issues/722) (CF Email Service sender)
+ [#732](https://github.com/jdrouet/catapulte/issues/732) (running fully on CF),
and mrml [#650](https://github.com/jdrouet/mrml/issues/650) (wasm fetch include
loader).

## Testing (native + CF share one test)

`scripts/smoke.nu <base-url>` is a single end-to-end smoke test (submit → confirm
delivery via `/events`) that runs against **any** catapulte — native or Workers,
since the HTTP API + lifecycle events are identical. Mirrors the upstream
`scripts/smoke-test.sh` assertion (real delivery) but URL-only, no mail-sink peek.

- `mise run test:local` — shared smoke vs a local compose stack (mailpit ⇒ PASS)
- `mise run test:cf` — shared smoke vs the deployed Worker (needs a verified sender for PASS)
- `mise run test:compose` — the upstream compose suite (mirrors `just test-compose`)
- `mise run verify` — fast API/A/B/C check (acceptance-level, no real delivery)
- `mise run cargo:test` — native unit/integration tests

Proven: the same `smoke.nu` → `SMOKE PASS` against native (mailpit), and correctly
reports `delivery.failed` against CF when the sender domain isn't verified.

## Live deployment

- URL: `https://catapulte-worker.gedw99.workers.dev` (account gedw99@gmail.com)
- One command to check it: **`mise run verify`** (URL from fnox `CATAPULTE_WORKER_URL` or env)
- Deploy: `mise run deploy` (one-time: `mise run deploy:bootstrap`). See `worker/DEPLOY.md`.

## Done ✅ (worker-runtime)

- [x] `router()` reuse — the worker serves the **real** `inbound-http` axum router (routes/auth/extractors), not hand-wired. axum/tokio/trace/timeout gated behind the `server` feature so it compiles to wasm; native unaffected (85+51 tests pass).
- [x] Storage + queue + scheduling = **one SQLite-backed Durable Object** (`outbound-do`). No D1, no CF Queues, no cron. `alarm()` drains with backoff.
- [x] Shared SQL wire format extracted to `outbound-sql-core` (sqlite/d1/do reuse it).
- [x] Rendering on wasm — minijinja (interpolate) + mrml (MJML→HTML), inline / named (R2 `TEMPLATES`) / remote (`worker::Fetch`).
- [x] Send via the CF Email Service `send_email` binding.
- [x] R2 attachments (`outbound-attachment-r2`) — store on submit, include in MIME on send.
- [x] Real status + lifecycle events (`GET /events`).
- [x] **A** — send from many domains: verify in CF + set `sender` (no code).
- [x] **B** — per-sender quotas + reporting (`CATAPULTE_SENDERS` env JSON; `/senders` usage; defer-over-quota at delivery).
- [x] **C** — multi-tenant DO sharding via `X-Catapulte-Tenant` header (isolated per tenant — verified).
- [x] Attachment GC — terminal-delete R2 blobs when an email is sent/failed.
- [x] wasm `std::time` panics fixed (tower-http layers gated; uuid v4 on wasm).
- [x] mise tasks + `mise run verify` (nushell, A/B/C) + `DEPLOY.md`.
- [x] D1 adapter (`outbound-d1`) kept as a multi-writer alternative to the DO.
- [x] **Outbound webhooks** (`CATAPULTE_WEBHOOK_URL` + optional `_TOKEN`) — composite `EventSink` records to the DO **and** POSTs each lifecycle event (Queued/Sent/Failed) to the webhook, best-effort (a down webhook never blocks email).
- [x] **MJML includes** (multi-loader) — `<mj-include path="header">` loads `<name>.mjml` from the TEMPLATES R2 bucket; `path="https://…"` is fetched via worker::Fetch. mrml async parse + a WorkerIncludeLoader (mrml async loader is ?Send on wasm). Verified live.
- [x] **Remote-template per-host auth** (`CATAPULTE_RESOLVER_AUTH` JSON host→header) — auth header added when fetching remote templates for matching hosts.

## Multi-tenant domains

Onboarding **is** scriptable after all — the CF API has
`POST /zones/{zone}/email/sending/subdomains` (the guides only show the
dashboard). So `mise run domain:add <subdomain>` onboards a sending subdomain;
CF auto-writes the cf-bounce DKIM/SPF/DMARC (the zone uses CF DNS) → zero-touch.

- `mise run domain:scan` — list every account zone's existing email setup (Sending/Routing/MX) to find a free one. Then `domain:add <subdomain>` / `domain:list` / `domain:dns <tag>` / `domain:rm <tag>` (needs `CATAPULTE_MAIL_ZONE`).
- **Two models, both now easy:** one shared domain + per-tenant addresses (simplest), OR per-tenant subdomains for reputation isolation (now scriptable, no longer manual).
- **Enforcement (done, in the Worker):** each tenant DO has an allowed-sender
  list; a disallowed `sender` is rejected with 403 before the router. Empty =
  unrestricted. Manage with `mise run tenant:allow <tenant> <pattern>...` /
  `mise run tenant:list <tenant>` (admin route `GET/PUT /admin/allowed-senders`,
  API-key gated). Verified live.
- Provisioning stays OUT of the send Worker (least privilege).

## Remaining (all optional / low-priority)

- [ ] Periodic orphan sweep for R2 — terminal-delete (done) covers the normal path; a cron sweep would catch DELETE/crash orphans, but needs per-tenant R2 prefixes (cross-tenant enumeration). Low priority.
- [ ] Webhook HMAC signing — currently optional bearer token only.
- [ ] **N/A on CF:** NATS inbound (`INBOUND_NATS_*`) — use CF Queues instead. OTEL (`CATAPULTE_OTEL`) — CF observability replaces it (`[observability]` + `wrangler tail`).

Everything functional is now ported. The remaining items are hardening, not features.

## To go fully live (operator, one-time)

1. `mise run deploy:bootstrap` — create the R2 buckets (attachments + templates).
2. `mise run deploy` → `mise run verify` (worker is live; delivery not yet).
3. Pick a sender domain: `mise run domain:scan` → set `CATAPULTE_MAIL_ZONE` to a
   free zone → `mise run domain:add mail.<zone>` (CF writes the DNS).
4. `fnox set -p keychain CATAPULTE_SMOKE_SENDER hello@mail.<zone>` →
   `mise run test:cf` → real `delivery.succeeded`.

## Gotchas worth remembering

- Worker crates (`worker/`, `outbound-do`, `outbound-d1`, `outbound-attachment-r2`) are wasm32-only → `exclude`d from the workspace; built via `worker-build`. `outbound-sql-core` is a normal member.
- `!Send` CF handles (D1/R2) bridged via `SendWrapper`/`SendFuture`; the DO's `SqlStorage` is already `Send+Sync` + sync `exec`, so it needs neither.
- mise renders task bodies with Tera → wrap nushell containing `{{ }}` in `{% raw %}…{% endraw %}`.
