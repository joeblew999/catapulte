//! Cloudflare Workers entrypoint for catapulte (slice 2).
//!
//! Storage + queue + scheduling are folded into ONE Durable Object
//! (`CatapulteStore`) backed by `catapulte-outbound-do`:
//! - the DO's embedded SQLite holds emails + the queue,
//! - `fetch` accepts submissions and lists,
//! - `alarm()` drains the queue with backoff retries — replacing CF Queues and
//!   cron entirely.
//!
//! No D1, no Queues, no cron binding. The `fetch` entrypoint just forwards to a
//! single DO instance ("default"); shard by sender/tenant later for throughput.
//!
//! NOT yet (next slice): real delivery. `deliver()` is the injection point for
//! rendering (MRML) + sending (the `send_email` binding); today it is a no-op
//! success so the queue machinery is exercised end to end.

use std::time::Duration;

use catapulte_domain::entity::body::{BodySource, MjmlSource, Plain};
use catapulte_domain::entity::email::{EmailId, RecipientKind};
use catapulte_domain::entity::envelope::Envelope;
use catapulte_domain::port::email_repository::{
    EmailRecord, EmailRepository, EmailStatus, ListEmailsParams,
};
use catapulte_outbound_do::DoStore;
use serde::{Deserialize, Serialize};
use worker::{
    Date, DurableObject, Env, Method, Request, Response, Result, State, durable_object, event,
};

const DO_BINDING: &str = "CATAPULTE_STORE";
const MAX_ATTEMPTS: i64 = 5;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    // One shared instance for now; shard by sender/tenant later for throughput.
    let stub = env
        .durable_object(DO_BINDING)?
        .id_from_name("default")?
        .get_stub()?;
    stub.fetch_with_request(req).await
}

#[durable_object]
pub struct CatapulteStore {
    state: State,
    // Held for the delivery injection point (send_email binding) in deliver().
    #[allow(dead_code)]
    env: Env,
}

impl DurableObject for CatapulteStore {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let store = DoStore::new(self.state.storage().sql());
        store.init_schema()?;

        let path = req.path();
        match (req.method(), path.as_str()) {
            (Method::Get, "/health/live") => Response::ok("ok"),
            (Method::Get, "/emails") => list_emails(&store).await,
            (Method::Post, "/emails") => self.submit_email(&mut req, &store).await,
            _ => Response::error("not found", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        let store = DoStore::new(self.state.storage().sql());
        store.init_schema()?;

        let now = i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX);
        for item in store.claim_due(now, 10)? {
            match deliver(&item.email_id).await {
                Ok(()) => store.dequeue(&item.email_id)?,
                // Give up after MAX_ATTEMPTS — drop from the queue.
                Err(_) if item.attempts + 1 >= MAX_ATTEMPTS => store.dequeue(&item.email_id)?,
                Err(_) => store.reschedule(&item.email_id, now + backoff_ms(item.attempts + 1))?,
            }
        }

        // Re-arm for the earliest still-pending entry.
        if let Some(next) = store.next_run_at()? {
            let delay = u64::try_from((next - now).max(0)).unwrap_or(0);
            self.state
                .storage()
                .set_alarm(Duration::from_millis(delay))
                .await?;
        }
        Response::ok("")
    }
}

impl CatapulteStore {
    async fn submit_email(&self, req: &mut Request, store: &DoStore) -> Result<Response> {
        let body: SubmitRequest = req.json().await?;
        let envelope = body.into_envelope().map_err(worker::Error::RustError)?;
        let id = EmailId::default();

        store
            .save(id, &envelope)
            .await
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        // Enqueue for immediate delivery and wake the alarm now.
        let now = i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX);
        store.enqueue(id, now)?;
        self.state
            .storage()
            .set_alarm(Duration::from_millis(0))
            .await?;

        Response::from_json(&serde_json::json!({ "id": id.as_uuid().to_string() }))
    }
}

async fn list_emails(store: &DoStore) -> Result<Response> {
    let records = store
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

/// INJECTION POINT for real delivery (next slice): render the MJML body and
/// send via the `send_email` binding. For now a no-op success so the queue
/// drains and the alarm/retry machinery is exercised end to end.
async fn deliver(_email_id: &str) -> Result<()> {
    Ok(())
}

/// Exponential backoff in ms, capped at 5 minutes.
fn backoff_ms(attempts: i64) -> i64 {
    let shift = attempts.clamp(0, 8);
    (1000_i64 << shift).min(300_000)
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
