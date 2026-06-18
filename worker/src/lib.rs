//! Cloudflare Workers entrypoint for catapulte (slice 1).
//!
//! Proof-of-life that catapulte's hexagonal core runs on Workers: a `fetch`
//! handler routes HTTP to the same domain types, backed by the D1 storage
//! adapter (`catapulte-outbound-d1`). It demonstrates the runtime, the D1
//! binding, and the `!Send` → `Send` bridge compiling and wiring together.
//!
//! What this is NOT yet (later slices): the full `inbound-http` axum router,
//! MRML rendering (`outbound-mjml` needs a wasm build), the send path
//! (`send_email` binding), the queue consumer (CF Queues) and attachment
//! storage (R2). Routes here are hand-wired and the send path only persists.

use catapulte_domain::entity::body::{BodySource, MjmlSource, Plain};
use catapulte_domain::entity::email::{EmailId, RecipientKind};
use catapulte_domain::entity::envelope::Envelope;
use catapulte_domain::port::email_repository::{
    EmailRecord, EmailRepository, EmailStatus, ListEmailsParams, SaveResult,
};
use catapulte_outbound_d1::D1Adapter;
use serde::{Deserialize, Serialize};
use worker::{Env, Request, Response, Result, RouteContext, Router, event};

const D1_BINDING: &str = "DB";

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    console_error_panic_hook::set_once();

    Router::new()
        .get("/health/live", |_req, _ctx| Response::ok("ok"))
        .get_async("/emails", |_req, ctx| async move { list_emails(&ctx).await })
        .post_async("/emails", |mut req, ctx| async move {
            submit_email(&mut req, &ctx).await
        })
        .run(req, env)
        .await
}

fn repo(ctx: &RouteContext<()>) -> Result<D1Adapter> {
    Ok(D1Adapter::new(ctx.env.d1(D1_BINDING)?))
}

async fn list_emails(ctx: &RouteContext<()>) -> Result<Response> {
    let records = repo(ctx)?
        .list_emails(ListEmailsParams {
            status: None,
            after_ms: None,
            before_ms: None,
            recipient: None,
            template: None,
            id: None,
            limit: 50,
            offset: 0,
        })
        .await
        .map_err(|e| worker::Error::RustError(e.to_string()))?;

    let out: Vec<EmailRecordDto> = records.into_iter().map(EmailRecordDto::from).collect();
    Response::from_json(&out)
}

async fn submit_email(req: &mut Request, ctx: &RouteContext<()>) -> Result<Response> {
    let body: SubmitRequest = req.json().await?;
    let envelope = body.into_envelope().map_err(worker::Error::RustError)?;
    let id = EmailId::default();

    let result = repo(ctx)?
        .save(id, &envelope)
        .await
        .map_err(|e| worker::Error::RustError(e.to_string()))?;

    let id = match result {
        SaveResult::Created(id) | SaveResult::Duplicate(id) => id,
    };
    Response::from_json(&serde_json::json!({ "id": id.as_uuid().to_string() }))
}

// --- request / response DTOs -------------------------------------------------

#[derive(Deserialize)]
struct SubmitRequest {
    sender: String,
    #[serde(default)]
    recipients: Vec<RecipientIn>,
    subject: Option<String>,
    body: BodyIn,
    #[serde(default)]
    params: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct RecipientIn {
    kind: String,
    address: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BodyIn {
    Plain { text: Option<String>, html: Option<String> },
    MjmlInline { source: String },
    MjmlNamed { name: String },
}

impl SubmitRequest {
    fn into_envelope(self) -> std::result::Result<Envelope, String> {
        let recipients = self
            .recipients
            .into_iter()
            .map(|r| Ok((parse_kind(&r.kind)?, r.address)))
            .collect::<std::result::Result<Vec<_>, String>>()?;

        let body = match self.body {
            BodyIn::Plain { text, html } => {
                BodySource::Plain(Plain::try_new(text, html).map_err(|e| e.to_string())?)
            }
            BodyIn::MjmlInline { source } => BodySource::Mjml(MjmlSource::Inline(source)),
            BodyIn::MjmlNamed { name } => BodySource::Mjml(MjmlSource::Named(name)),
        };

        Ok(Envelope {
            idempotency_key: None,
            correlation_id: None,
            subject: self.subject,
            sender: self.sender,
            recipients,
            body,
            variables: self.params,
            attachments: vec![],
        })
    }
}

fn parse_kind(kind: &str) -> std::result::Result<RecipientKind, String> {
    match kind {
        "to" => Ok(RecipientKind::To),
        "cc" => Ok(RecipientKind::Cc),
        "bcc" => Ok(RecipientKind::Bcc),
        other => Err(format!("invalid recipient kind: {other}")),
    }
}

#[derive(Serialize)]
struct RecipientOut {
    kind: String,
    address: String,
}

#[derive(Serialize)]
struct EmailRecordDto {
    id: String,
    idempotency_key: Option<String>,
    subject: Option<String>,
    sender: String,
    recipients: Vec<RecipientOut>,
    created_at_ms: i64,
    status: String,
}

impl From<EmailRecord> for EmailRecordDto {
    fn from(r: EmailRecord) -> Self {
        Self {
            id: r.id.as_uuid().to_string(),
            idempotency_key: r.idempotency_key,
            subject: r.subject,
            sender: r.sender,
            recipients: r
                .recipients
                .into_iter()
                .map(|(k, address)| RecipientOut {
                    kind: kind_str(k).to_owned(),
                    address,
                })
                .collect(),
            created_at_ms: r.created_at_ms,
            status: status_str(r.status).to_owned(),
        }
    }
}

const fn kind_str(k: RecipientKind) -> &'static str {
    match k {
        RecipientKind::To => "to",
        RecipientKind::Cc => "cc",
        RecipientKind::Bcc => "bcc",
    }
}

const fn status_str(s: EmailStatus) -> &'static str {
    match s {
        EmailStatus::Queued => "queued",
        EmailStatus::Sent => "sent",
        EmailStatus::Failed => "failed",
    }
}
