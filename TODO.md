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

Upstream issue: https://github.com/jdrouet/catapulte/issues/722 (comment posted).

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

## Remaining (all optional / niche)

- [ ] **Outbound webhooks** (`WEBHOOK_*`) — POST delivery events to a configured URL. *The only remaining item with real value.* Plan: a `WEBHOOK_URL` env var; in the alarm after publishing Sent/Failed, `worker::Fetch` POST the event (best-effort), or a composite EventPublisher.
- [ ] MJML includes (`INCLUDE_LOADER_*`) — `<mj-include>` partials. Minor; needs an mrml include loader backed by R2/Fetch.
- [ ] Remote-template per-host auth (`RESOLVER_*`) — auth headers when fetching remote templates. Minor.
- [ ] Periodic orphan sweep for R2 — terminal-delete covers the normal path; a cron sweep would catch DELETE/crash orphans, but needs per-tenant R2 prefixes (cross-tenant enumeration). Low priority.
- [ ] **N/A on CF:** NATS inbound (`INBOUND_NATS_*`) — use CF Queues instead. OTEL (`CATAPULTE_OTEL`) — CF observability replaces it (`[observability]` + `wrangler tail`).

## To go fully live (operator, one-time)

1. Verify a sender domain in the CF dashboard (Email) — required for real delivery.
2. `mise run deploy:bootstrap` (creates the R2 bucket).
3. `mise run deploy` → `mise run verify`.

## Gotchas worth remembering

- Worker crates (`worker/`, `outbound-do`, `outbound-d1`, `outbound-attachment-r2`) are wasm32-only → `exclude`d from the workspace; built via `worker-build`. `outbound-sql-core` is a normal member.
- `!Send` CF handles (D1/R2) bridged via `SendWrapper`/`SendFuture`; the DO's `SqlStorage` is already `Send+Sync` + sync `exec`, so it needs neither.
- mise renders task bodies with Tera → wrap nushell containing `{{ }}` in `{% raw %}…{% endraw %}`.
