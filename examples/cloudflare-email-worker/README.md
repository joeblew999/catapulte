# Cloudflare Email Worker (companion to `outbound-cloudflare`)

catapulte runs as a native server and cannot hold a Cloudflare `send_email`
binding (that only exists inside a Worker). This ~40-line Worker bridges the
gap: catapulte POSTs the rendered RFC822 message here, and the Worker forwards
it to Cloudflare's Email Service.

```
catapulte (Hetzner/container)            Cloudflare
┌──────────────────────────┐            ┌──────────────────────┐
│ MRML render + routing     │  POST      │ this Worker          │
│ outbound-cloudflare ──────┼──RFC822───▶│ env.SEB.send(msg) ──▶│ Email Service
└──────────────────────────┘  +Bearer   └──────────────────────┘
```

## Deploy

```sh
wrangler deploy
wrangler secret put CATAPULTE_TOKEN   # optional shared secret
```

Then point catapulte at it (per-sender config, mirrors the SMTP sender vars):

```sh
CATAPULTE_SENDER_CF_WORKER_URL=https://catapulte-email-worker.<you>.workers.dev
CATAPULTE_SENDER_CF_TOKEN=<the same secret>
```

## Caveats

- **Recipients must be deliverable by your binding.** The Email *Routing*
  `send_email` binding only sends to addresses verified in your zone. The newer
  Email *Sending* product lifts that; the Worker code is identical, only the
  binding wiring in `wrangler.toml` changes.
- The Worker parses `From`/`To` out of the MIME headers to build the
  `EmailMessage` envelope. catapulte always sets both.
- This is an example, not hardened: add rate limiting / IP allowlisting as
  needed before production use.
