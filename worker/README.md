# catapulte on Cloudflare Workers

catapulte's hexagonal core running on Workers via `workers-rs`, with **one
Durable Object** providing storage + queue + scheduling.

## Architecture

```
fetch ──▶ CatapulteStore (Durable Object)
            ├─ embedded SQLite  (emails + email_queue)   ← replaces D1
            ├─ alarm()          (drain queue, backoff)   ← replaces CF Queues + cron
            └─ serialized exec  (no row-claim races)
```

One primitive instead of D1 + Queues + cron. It maps 1:1 onto catapulte's
native "DB-as-queue + background poller" model — the DO `alarm()` *is* the
poller, and the DO's single-threaded execution removes the `claimed_until`
race the native sqlite backend handles by hand.

Crates:
- `adapter/outbound-sql-core` — wire DTOs shared by sqlite / D1 / DO.
- `adapter/outbound-do` — `DoStore`: `EmailRepository` + `EmailQueue` +
  `EventRepository`/`EventPublisher` + `SenderUsage` over DO SQLite. No
  `SendWrapper` needed (`SqlStorage` is `Send+Sync`, `exec` is sync).
- `adapter/outbound-attachment-r2` — `AttachmentStore` over R2.
- `adapter/outbound-d1` — D1 alternative (kept for multi-writer setups).
- `worker/` — `#[durable_object]` + `fetch`/`alarm`, serving the **real**
  `inbound-http` axum `router()` over the same domain use-cases.

## What works (feature-complete vs native)

- The full HTTP API — the real `inbound-http` router (routes, bearer auth,
  extractors, body limits), not hand-wired: `POST /emails` (+ `/batch`),
  `GET /emails`, `/emails/{id}/events`, `/events`, `/senders`, `/health/*`.
- Submit → `SubmitEmailService` (save + enqueue + event); `alarm()` drains with
  capped backoff, renders, sends, records status + lifecycle events.
- Rendering: minijinja interpolation + mrml MJML, with `<mj-include>` partials
  (named → `TEMPLATES` R2 bucket, URL → `worker::Fetch`).
- Send via the Email Service `send_email` binding.
- R2 attachments (stored on submit, in the MIME on send, GC'd at terminal).
- Real status + lifecycle events; optional **webhooks** for event fan-out.
- Multi-tenant DO sharding (`X-Catapulte-Tenant`), per-sender quotas
  (`CATAPULTE_SENDERS`), per-host remote-template auth (`CATAPULTE_RESOLVER_AUTH`).

Caveat: real delivery needs a sender domain verified in your CF account.

## Remaining (hardening, not features)

See the repo-root `TODO.md`. Briefly: R2 orphan-sweep cron, webhook HMAC
signing. N/A on CF: NATS inbound (use CF Queues), OTEL (CF observability).

## Deploy

```sh
mise run worker:deploy        # wrangler deploy — provisions the DO + SQLite migration
mise run worker:tail          # logs
```

Local build only (no account needed):

```sh
mise run worker:build         # worker-build --release → build/worker/shim.mjs
```

The DO's SQLite schema is created in code (`DoStore::init_schema`, run on every
activation) — there is no D1 database to create and no SQL migration files.

## Why these crates live outside the cargo workspace

`worker/`, `adapter/outbound-do/` and `adapter/outbound-d1/` are `wasm32`-only
and listed under `exclude` in the root `Cargo.toml`, so native
`cargo build --workspace` stays green. They build via `worker-build`.
`outbound-sql-core` *is* a normal member (pure serde, compiles both ways).
