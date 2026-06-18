import { EmailMessage } from "cloudflare:email";

// Companion Worker for catapulte's `outbound-cloudflare` adapter.
//
// catapulte renders the email to raw RFC822 and POSTs the bytes here. This
// Worker holds the `send_email` binding (catapulte cannot, it is not a Worker)
// and forwards the message to Cloudflare's Email Service.
//
// The adapter sends `From`/`To` in the MIME headers; we parse them back out to
// construct the EmailMessage envelope the binding requires.
export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }

    // Optional shared-secret check, matched to the adapter's `_TOKEN`.
    if (env.CATAPULTE_TOKEN) {
      const auth = request.headers.get("authorization") ?? "";
      if (auth !== `Bearer ${env.CATAPULTE_TOKEN}`) {
        return new Response("unauthorized", { status: 401 });
      }
    }

    const raw = await request.text();
    const from = headerValue(raw, "from");
    const to = headerValue(raw, "to");
    if (!from || !to) {
      return new Response("missing From/To header in message", { status: 400 });
    }

    try {
      const message = new EmailMessage(from, to, raw);
      await env.SEB.send(message);
    } catch (err) {
      return new Response(`send failed: ${err}`, { status: 502 });
    }
    return new Response(null, { status: 202 });
  },
};

function headerValue(raw, name) {
  const re = new RegExp(`^${name}:\\s*(.+)$`, "im");
  const match = raw.match(re);
  if (!match) return null;
  // Strip a display name if present, keeping the bare address.
  const angle = match[1].match(/<([^>]+)>/);
  return (angle ? angle[1] : match[1]).trim();
}
