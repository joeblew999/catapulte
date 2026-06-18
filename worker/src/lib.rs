//! Cloudflare Workers entrypoint for catapulte — full app on Workers.
//!
//! Storage + queue + scheduling are one SQLite-backed Durable Object
//! (`CatapulteStore`, via `catapulte-outbound-do`): no D1, no CF Queues, no
//! cron. The HTTP surface is the *real* `catapulte-inbound-http::router()` —
//! same routes, auth middleware, extractors and body limits as the native
//! server — served inside the DO over the same domain use-cases.
//!
//! Request flow: `fetch` forwards to a single DO instance ("default"); the DO
//! builds an app state over its SQLite and serves the axum router. Submissions
//! go through `SubmitEmailService` (save + enqueue + event); the DO `alarm()`
//! drains the queue, renders (minijinja + mrml) and sends via the Email Service
//! `send_email` binding, with capped exponential backoff and real status.
//!
//! Worker-shaped ports: storage/queue/usage/health are the DO's SQLite
//! (`outbound-do`); the clock is `Date::now()`; events aren't recorded yet
//! (status lives in `emails.status`); attachments aren't accepted on this path
//! yet (the attachment ports are inert stubs).

use std::sync::Arc;
use std::time::Duration;

use catapulte_domain::entity::attachment::BlobRef;
use catapulte_domain::entity::body::{BodySource, MjmlSource};
use catapulte_domain::entity::lifecycle_event::LifecycleEvent;
use catapulte_domain::port::attachment_fetcher::{AttachmentFetchError, AttachmentFetcher};
use catapulte_domain::port::attachment_store::{
    AttachmentReader, AttachmentStore, AttachmentStoreError, PutResult,
};
use catapulte_domain::port::clock::Clock;
use catapulte_domain::port::event_publisher::{EventPublisher, EventPublisherError};
use catapulte_domain::use_case::check_readiness::{CheckReadinessService, CheckReadinessUseCase};
use catapulte_domain::use_case::list_emails::{ListEmailsService, ListEmailsUseCase};
use catapulte_domain::use_case::list_events::{ListEventsService, ListEventsUseCase};
use catapulte_domain::use_case::list_senders::{ListSendersService, ListSendersUseCase};
use catapulte_domain::use_case::submit_email::{SubmitEmailService, SubmitEmailUseCase};
use catapulte_inbound_http::{HttpServerState, ReadinessState, router};
use catapulte_outbound_do::DoStore;
use tower::ServiceExt;
use worker::{
    Date, DurableObject, EmailMessage, Env, Headers, Request, Response, Result, State, Storage,
    durable_object, event,
};

const DO_BINDING: &str = "CATAPULTE_STORE";
const EMAIL_BINDING: &str = "EMAIL";
const TEMPLATES_BINDING: &str = "TEMPLATES";
const API_KEY_VAR: &str = "CATAPULTE_HTTP_API_KEY";
const MAX_ATTEMPTS: i64 = 5;
const REQUEST_TIMEOUT_SECS: u64 = 30;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let stub = env
        .durable_object(DO_BINDING)?
        .id_from_name("default")?
        .get_stub()?;
    stub.fetch_with_request(req).await
}

// --- worker-shaped port adapters --------------------------------------------

/// Clock backed by the Workers runtime (the domain `SystemClock` panics on wasm).
struct WasmClock;
impl Clock for WasmClock {
    fn now_ms(&self) -> i64 {
        i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX)
    }
}

/// Events aren't recorded yet (status lives in `emails.status`).
struct NoopEventPublisher;
impl EventPublisher for NoopEventPublisher {
    fn publish(
        &self,
        _event: &LifecycleEvent,
    ) -> impl std::future::Future<Output = std::result::Result<(), EventPublisherError>> + Send {
        async { Ok(()) }
    }
}

/// Attachments aren't accepted on the worker submit path yet — inert.
struct NoAttachmentStore;
impl AttachmentStore for NoAttachmentStore {
    fn put(
        &self,
        _reader: AttachmentReader,
    ) -> impl std::future::Future<Output = std::result::Result<PutResult, AttachmentStoreError>> + Send
    {
        async {
            Err(AttachmentStoreError::Io {
                source: anyhow::anyhow!("attachments are not supported on the worker yet"),
            })
        }
    }

    fn get(
        &self,
        _blob: &BlobRef,
    ) -> impl std::future::Future<Output = std::result::Result<AttachmentReader, AttachmentStoreError>>
    + Send {
        async { Err(AttachmentStoreError::NotFound) }
    }

    fn delete(
        &self,
        _blob: &BlobRef,
    ) -> impl std::future::Future<Output = std::result::Result<(), AttachmentStoreError>> + Send {
        async { Ok(()) }
    }
}

struct NoAttachmentFetcher;
impl AttachmentFetcher for NoAttachmentFetcher {
    fn fetch(
        &self,
        _url: &url::Url,
    ) -> impl std::future::Future<Output = std::result::Result<AttachmentReader, AttachmentFetchError>>
    + Send {
        async {
            Err(AttachmentFetchError::Fetch {
                source: anyhow::anyhow!("attachment fetch is not supported on the worker yet"),
            })
        }
    }
}

// --- app state (the real HttpServerState) -----------------------------------

type SubmitSvc =
    SubmitEmailService<DoStore, DoStore, NoopEventPublisher, NoAttachmentStore, NoAttachmentFetcher>;

#[derive(Clone)]
struct AppState {
    submit_email: Arc<SubmitSvc>,
    list_emails: Arc<ListEmailsService<DoStore>>,
    list_events: Arc<ListEventsService<DoStore>>,
    list_senders: Arc<ListSendersService<DoStore, WasmClock>>,
    check_readiness: Arc<CheckReadinessService<DoStore>>,
}

impl AppState {
    /// Builds the state over the DO's SQLite. Each service gets its own
    /// `DoStore` handle (all point at the same embedded database).
    fn new(storage: &Storage) -> Self {
        Self {
            submit_email: Arc::new(SubmitEmailService::new(
                DoStore::new(storage.sql()),
                DoStore::new(storage.sql()),
                NoopEventPublisher,
                NoAttachmentStore,
                NoAttachmentFetcher,
            )),
            list_emails: Arc::new(ListEmailsService::new(DoStore::new(storage.sql()))),
            list_events: Arc::new(ListEventsService::new(DoStore::new(storage.sql()))),
            list_senders: Arc::new(ListSendersService::new(
                Vec::new(),
                DoStore::new(storage.sql()),
                WasmClock,
            )),
            check_readiness: Arc::new(CheckReadinessService::new(DoStore::new(storage.sql()))),
        }
    }
}

impl ReadinessState for AppState {
    fn check_readiness(&self) -> &impl CheckReadinessUseCase {
        self.check_readiness.as_ref()
    }
}

impl HttpServerState for AppState {
    fn submit_email(&self) -> &impl SubmitEmailUseCase {
        self.submit_email.as_ref()
    }
    fn list_emails(&self) -> &impl ListEmailsUseCase {
        self.list_emails.as_ref()
    }
    fn list_events(&self) -> &impl ListEventsUseCase {
        self.list_events.as_ref()
    }
    fn list_senders(&self) -> &impl ListSendersUseCase {
        self.list_senders.as_ref()
    }
}

// --- the Durable Object ------------------------------------------------------

#[durable_object]
pub struct CatapulteStore {
    state: State,
    env: Env,
}

impl DurableObject for CatapulteStore {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let storage = self.state.storage();
        DoStore::new(storage.sql()).init_schema()?;

        // Serve the real catapulte router over the DO-backed app state.
        let api_key = self
            .env
            .var(API_KEY_VAR)
            .ok()
            .map(|v| v.to_string())
            .filter(|v| !v.is_empty());
        let app = router(
            AppState::new(&storage),
            api_key,
            Duration::from_secs(REQUEST_TIMEOUT_SECS),
        );

        let axum_req = to_axum_request(&mut req).await?;
        let axum_resp = app
            .oneshot(axum_req)
            .await
            .expect("axum router is infallible");

        // A submission enqueues via the use-case; arm the alarm so the queue is
        // drained promptly.
        if let Ok(Some(next)) = DoStore::new(storage.sql()).next_run_at() {
            let now = i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX);
            let delay = u64::try_from((next - now).max(0)).unwrap_or(0);
            storage.set_alarm(Duration::from_millis(delay)).await?;
        }

        to_worker_response(axum_resp).await
    }

    async fn alarm(&self) -> Result<Response> {
        let store = DoStore::new(self.state.storage().sql());
        store.init_schema()?;

        let now = i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX);
        for item in store.claim_due(now, 10)? {
            match self.deliver(&store, &item.email_id).await {
                Ok(()) => {
                    store.set_status(&item.email_id, "sent")?;
                    store.dequeue(&item.email_id)?;
                }
                Err(_) if item.attempts + 1 >= MAX_ATTEMPTS => {
                    store.set_status(&item.email_id, "failed")?;
                    store.dequeue(&item.email_id)?;
                }
                Err(_) => store.reschedule(&item.email_id, now + backoff_ms(item.attempts + 1))?,
            }
        }

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
    /// Renders the email and sends it via the Email Service binding — one
    /// message per recipient. The sender domain must be verified in CF.
    async fn deliver(&self, store: &DoStore, email_id: &str) -> Result<()> {
        let Some(email) = store
            .load_envelope(email_id)
            .map_err(|e| worker::Error::RustError(e.to_string()))?
        else {
            return Ok(());
        };

        let (text, html) = self
            .render(&email.body, &email.variables)
            .await
            .map_err(worker::Error::RustError)?;
        let subject = email.subject.as_deref().unwrap_or("");
        let sender = self.env.send_email(EMAIL_BINDING)?;

        for (_, to) in &email.recipients {
            let raw = build_mime(&email.sender, to, subject, text.as_deref(), html.as_deref());
            let message = EmailMessage::new(&email.sender, to, &raw)?;
            sender.send(&message).await?;
        }
        Ok(())
    }

    async fn render(
        &self,
        body: &BodySource,
        vars: &serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<(Option<String>, Option<String>), String> {
        match body {
            BodySource::Plain(p) => {
                let text = p.text().map(|t| interpolate(t, vars)).transpose()?;
                let html = p.html().map(|h| interpolate(h, vars)).transpose()?;
                Ok((text, html))
            }
            BodySource::Mjml(MjmlSource::Inline(src)) => {
                Ok((None, Some(render_mjml(&interpolate(src, vars)?)?)))
            }
            BodySource::Mjml(MjmlSource::Named(name)) => {
                let src = self.fetch_named_template(name).await?;
                Ok((None, Some(render_mjml(&interpolate(&src, vars)?)?)))
            }
            BodySource::Mjml(MjmlSource::Remote(url)) => {
                let src = fetch_remote_template(url).await?;
                Ok((None, Some(render_mjml(&interpolate(&src, vars)?)?)))
            }
        }
    }

    async fn fetch_named_template(&self, name: &str) -> std::result::Result<String, String> {
        let bucket = self
            .env
            .bucket(TEMPLATES_BINDING)
            .map_err(|e| format!("TEMPLATES bucket binding missing: {e}"))?;
        let object = bucket
            .get(format!("{name}.mjml"))
            .execute()
            .await
            .map_err(|e| format!("fetching template {name}: {e}"))?
            .ok_or_else(|| format!("named template not found: {name}"))?;
        object
            .body()
            .ok_or_else(|| format!("named template {name} has no body"))?
            .text()
            .await
            .map_err(|e| format!("reading template {name}: {e}"))
    }
}

/// Converts a Workers request into an axum request the reused router can serve.
async fn to_axum_request(req: &mut Request) -> Result<axum::http::Request<axum::body::Body>> {
    let method = axum::http::Method::from_bytes(req.method().to_string().as_bytes())
        .unwrap_or(axum::http::Method::GET);
    let uri = req.url().map_or_else(|_| req.path(), |u| u.to_string());
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    for (name, value) in req.headers() {
        builder = builder.header(name, value);
    }
    let body = req.bytes().await?;
    builder
        .body(axum::body::Body::from(body))
        .map_err(|e| worker::Error::RustError(format!("building request: {e}")))
}

/// Converts the axum response back into a Workers response.
async fn to_worker_response(
    resp: axum::http::Response<axum::body::Body>,
) -> Result<Response> {
    let (parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| worker::Error::RustError(format!("reading response body: {e}")))?;
    let headers = Headers::new();
    for (name, value) in &parts.headers {
        if let Ok(v) = value.to_str() {
            let _ = headers.set(name.as_str(), v);
        }
    }
    Ok(Response::from_bytes(bytes.to_vec())?
        .with_status(parts.status.as_u16())
        .with_headers(headers))
}

async fn fetch_remote_template(url: &url::Url) -> std::result::Result<String, String> {
    let mut resp = worker::Fetch::Url(url.clone())
        .send()
        .await
        .map_err(|e| format!("fetching remote template: {e}"))?;
    if resp.status_code() != 200 {
        return Err(format!("remote template returned HTTP {}", resp.status_code()));
    }
    resp.text()
        .await
        .map_err(|e| format!("reading remote template: {e}"))
}

fn interpolate(
    template: &str,
    vars: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<String, String> {
    let env = minijinja::Environment::new();
    env.render_str(template, minijinja::Value::from_serialize(vars))
        .map_err(|e| format!("interpolation failed: {e}"))
}

fn render_mjml(mjml: &str) -> std::result::Result<String, String> {
    let parsed = mrml::parse(mjml).map_err(|e| format!("mjml parse failed: {e}"))?;
    parsed
        .element
        .render(&mrml::prelude::render::RenderOptions::default())
        .map_err(|e| format!("mjml render failed: {e}"))
}

/// Builds a minimal RFC822 message (CRLF, multipart/alternative when both parts
/// are present).
fn build_mime(from: &str, to: &str, subject: &str, text: Option<&str>, html: Option<&str>) -> String {
    let headers = format!("From: {from}\r\nTo: {to}\r\nSubject: {subject}\r\nMIME-Version: 1.0\r\n");
    match (text, html) {
        (Some(t), Some(h)) => {
            let b = "catapulte-alt-boundary";
            format!(
                "{headers}Content-Type: multipart/alternative; boundary=\"{b}\"\r\n\r\n\
                 --{b}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{t}\r\n\
                 --{b}\r\nContent-Type: text/html; charset=utf-8\r\n\r\n{h}\r\n--{b}--\r\n"
            )
        }
        (None, Some(h)) => format!("{headers}Content-Type: text/html; charset=utf-8\r\n\r\n{h}\r\n"),
        (Some(t), None) => format!("{headers}Content-Type: text/plain; charset=utf-8\r\n\r\n{t}\r\n"),
        (None, None) => format!("{headers}Content-Type: text/plain; charset=utf-8\r\n\r\n\r\n"),
    }
}

/// Exponential backoff in ms, capped at 5 minutes.
fn backoff_ms(attempts: i64) -> i64 {
    let shift = attempts.clamp(0, 8);
    (1000_i64 << shift).min(300_000)
}
