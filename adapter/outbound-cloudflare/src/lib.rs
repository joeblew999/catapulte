//! Cloudflare Email Service outbound transport.
//!
//! Cloudflare's email sending is not an SMTP host or a public REST endpoint you
//! can reach from an arbitrary box — it is only available from inside a Worker,
//! through the `send_email` binding, which accepts a raw RFC822 message. So this
//! adapter renders the email to the exact same MIME bytes the SMTP adapter would
//! send (via `lettre` as a builder), then POSTs them to a small companion Worker
//! that forwards them to the binding. See `examples/cloudflare-email-worker`.
//!
//! catapulte keeps owning templating, routing and retries; Cloudflare is purely
//! the transport on the far side. The adapter implements the same
//! [`EmailTransport`] port as `outbound-smtp`, so it is a drop-in alternative.

use anyhow::Context;
use catapulte_domain::entity::body::RenderedBody;
use catapulte_domain::entity::email::RecipientKind;
use catapulte_domain::port::email_sender::OutboundEmail;
use catapulte_domain::port::email_transport::EmailTransport;
use lettre::Address;
use lettre::message::header::{ContentDisposition, ContentType};
use lettre::message::{Mailbox, Message, MultiPart, SinglePart};

/// Configuration for the Cloudflare Email transport.
pub struct CloudflareConfig {
    /// HTTPS URL of the companion Worker that holds the `send_email` binding.
    pub worker_url: String,
    /// Optional bearer token the Worker checks before forwarding to the binding.
    pub api_token: Option<String>,
}

impl CloudflareConfig {
    /// Builds the config from environment variables, mirroring the per-sender
    /// convention used by the SMTP adapter:
    ///
    /// - `{prefix}_WORKER_URL` (required)
    /// - `{prefix}_TOKEN` (optional)
    ///
    /// # Errors
    ///
    /// Returns an error if the required `_WORKER_URL` variable is missing.
    pub fn from_env(prefix: &str) -> anyhow::Result<Self> {
        Self::from_lookup(prefix, |key| std::env::var(key))
    }

    pub(crate) fn from_lookup<F>(prefix: &str, lookup: F) -> anyhow::Result<Self>
    where
        F: Fn(&str) -> Result<String, std::env::VarError>,
    {
        let url_key = format!("{prefix}_WORKER_URL");
        let worker_url = lookup(&url_key).with_context(|| format!("missing env var {url_key}"))?;
        let api_token = lookup(&format!("{prefix}_TOKEN")).ok();
        Ok(Self {
            worker_url,
            api_token,
        })
    }

    /// Builds the transport.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be constructed.
    pub fn build(self) -> anyhow::Result<CloudflareTransport> {
        let client = reqwest::Client::builder()
            .build()
            .context("building http client")?;
        Ok(CloudflareTransport {
            client,
            worker_url: self.worker_url,
            api_token: self.api_token,
        })
    }
}

/// Sends rendered emails through a Cloudflare Worker's `send_email` binding.
pub struct CloudflareTransport {
    client: reqwest::Client,
    worker_url: String,
    api_token: Option<String>,
}

impl CloudflareTransport {
    async fn send_inner(&self, email: &OutboundEmail) -> anyhow::Result<()> {
        let message = build_message(email)?;
        let raw = message.formatted();

        let mut request = self
            .client
            .post(&self.worker_url)
            .header(reqwest::header::CONTENT_TYPE, "message/rfc822")
            .body(raw);
        if let Some(token) = &self.api_token {
            request = request.bearer_auth(token);
        }

        let response = request
            .send()
            .await
            .context("posting message to cloudflare email worker")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("cloudflare email worker returned {status}: {body}");
        }
        Ok(())
    }
}

impl EmailTransport for CloudflareTransport {
    fn deliver<'a>(
        &'a self,
        email: &'a OutboundEmail,
    ) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send + 'a {
        self.send_inner(email)
    }
}

// --- MIME building -----------------------------------------------------------
// Mirrors `catapulte-outbound-smtp::transport` so both adapters emit byte-for-byte
// identical messages. A future refactor could lift these helpers into a shared
// crate; duplicated here to keep this adapter additive and self-contained.

fn parse_mailbox(addr: &str) -> anyhow::Result<Mailbox> {
    addr.parse::<Address>()
        .with_context(|| format!("invalid address: {addr}"))
        .map(|a| Mailbox::new(None, a))
}

fn apply_recipients(
    mut builder: lettre::message::MessageBuilder,
    recipients: &[(RecipientKind, String)],
) -> anyhow::Result<lettre::message::MessageBuilder> {
    for (kind, address) in recipients {
        let mailbox = parse_mailbox(address)?;
        builder = match kind {
            RecipientKind::To => builder.to(mailbox),
            RecipientKind::Cc => builder.cc(mailbox),
            RecipientKind::Bcc => builder.bcc(mailbox),
        };
    }
    Ok(builder)
}

fn build_body_part(body: &RenderedBody) -> MultiPart {
    let mut part = MultiPart::alternative().build();
    if let Some(text) = body.text() {
        part = part.singlepart(
            SinglePart::builder()
                .header(ContentType::TEXT_PLAIN)
                .body(text.to_owned()),
        );
    }
    if let Some(html) = body.html() {
        part = part.singlepart(
            SinglePart::builder()
                .header(ContentType::TEXT_HTML)
                .body(html.to_owned()),
        );
    }
    part
}

fn build_message(email: &OutboundEmail) -> anyhow::Result<Message> {
    let from = parse_mailbox(&email.sender)?;
    let mut builder = apply_recipients(Message::builder().from(from), &email.recipients)?;
    if let Some(subject) = email.subject.as_deref() {
        builder = builder.subject(subject);
    }

    let body_part = build_body_part(&email.body);
    if email.attachments.is_empty() {
        return builder
            .multipart(body_part)
            .context("building email message");
    }

    let mut mixed = MultiPart::mixed().multipart(body_part);
    for att in &email.attachments {
        let content_type = ContentType::parse(&att.content_type)
            .with_context(|| format!("invalid attachment content-type: {}", att.content_type))?;
        mixed = mixed.singlepart(
            SinglePart::builder()
                .header(content_type)
                .header(ContentDisposition::attachment(&att.filename))
                .body(att.bytes.to_vec()),
        );
    }
    builder
        .multipart(mixed)
        .context("building email message with attachments")
}

#[cfg(test)]
mod tests {
    use super::*;
    use catapulte_domain::entity::body::{Plain, RenderedBody};
    use catapulte_domain::entity::email::RecipientKind;
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_email() -> OutboundEmail {
        let plain =
            Plain::try_new(Some("hello".to_owned()), Some("<p>hello</p>".to_owned())).unwrap();
        OutboundEmail {
            sender: "from@example.com".to_owned(),
            subject: Some("hi".to_owned()),
            recipients: vec![(RecipientKind::To, "to@example.com".to_owned())],
            body: RenderedBody::new(plain),
            attachments: vec![],
        }
    }

    #[test]
    fn from_lookup_requires_worker_url() {
        let err = CloudflareConfig::from_lookup("PFX", |_| Err(std::env::VarError::NotPresent))
            .err()
            .expect("expected a missing-var error");
        assert!(err.to_string().contains("PFX_WORKER_URL"));
    }

    #[test]
    fn build_message_produces_rfc822_with_recipient() {
        let msg = build_message(&sample_email()).unwrap();
        let raw = String::from_utf8(msg.formatted()).unwrap();
        assert!(raw.contains("To: to@example.com"));
        assert!(raw.contains("Subject: hi"));
        assert!(raw.contains("<p>hello</p>"));
    }

    #[tokio::test]
    async fn deliver_posts_rfc822_to_worker() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("content-type", "message/rfc822"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let transport = CloudflareConfig {
            worker_url: server.uri(),
            api_token: Some("secret".to_owned()),
        }
        .build()
        .unwrap();

        transport.deliver(&sample_email()).await.unwrap();
    }

    #[tokio::test]
    async fn deliver_propagates_worker_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
            .mount(&server)
            .await;

        let transport = CloudflareConfig {
            worker_url: server.uri(),
            api_token: None,
        }
        .build()
        .unwrap();

        let err = transport.deliver(&sample_email()).await.unwrap_err();
        assert!(err.to_string().contains("500"));
    }
}
