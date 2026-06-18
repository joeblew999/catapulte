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

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use send_wrapper::SendWrapper;

use base64::Engine;
use catapulte_domain::entity::body::{BodySource, MjmlSource};
use catapulte_domain::entity::email::EmailId;
use catapulte_domain::entity::error_class::ErrorClass;
use catapulte_domain::entity::lifecycle_event::LifecycleEvent;
use catapulte_domain::entity::sender::{QuotaRange, SenderConfig, SenderName, SenderQuota};
use catapulte_domain::port::sender_usage::SenderUsage;
use catapulte_domain::port::attachment_fetcher::{AttachmentFetchError, AttachmentFetcher};
use catapulte_domain::port::attachment_store::{AttachmentReader, AttachmentStore};
use catapulte_domain::port::clock::Clock;
use catapulte_domain::port::event_publisher::{EventPublisher, EventPublisherError};
use catapulte_domain::use_case::check_readiness::{CheckReadinessService, CheckReadinessUseCase};
use catapulte_domain::use_case::list_emails::{ListEmailsService, ListEmailsUseCase};
use catapulte_domain::use_case::list_events::{ListEventsService, ListEventsUseCase};
use catapulte_domain::use_case::list_senders::{ListSendersService, ListSendersUseCase};
use catapulte_domain::use_case::submit_email::{SubmitEmailService, SubmitEmailUseCase};
use catapulte_inbound_http::{HttpServerState, ReadinessState, router};
use catapulte_outbound_attachment_r2::R2AttachmentStore;
use catapulte_outbound_do::DoStore;
use tower::ServiceExt;
use worker::wasm_bindgen::JsValue;
use worker::{
    Date, DurableObject, EmailMessage, Env, Fetch, Headers, Method, Request, RequestInit, Response,
    Result, State, Storage, durable_object, event,
};

const DO_BINDING: &str = "CATAPULTE_STORE";
const EMAIL_BINDING: &str = "EMAIL";
const TEMPLATES_BINDING: &str = "TEMPLATES";
const ATTACHMENTS_BINDING: &str = "ATTACHMENTS";
const API_KEY_VAR: &str = "CATAPULTE_HTTP_API_KEY";
const WEBHOOK_URL_VAR: &str = "CATAPULTE_WEBHOOK_URL";
const WEBHOOK_TOKEN_VAR: &str = "CATAPULTE_WEBHOOK_TOKEN";
const RESOLVER_AUTH_VAR: &str = "CATAPULTE_RESOLVER_AUTH";
const TENANT_HEADER: &str = "X-Catapulte-Tenant";
const DEFAULT_TENANT: &str = "default";
const MAX_ATTEMPTS: i64 = 5;
const REQUEST_TIMEOUT_SECS: u64 = 30;

/// (C) Multi-tenant sharding: each tenant gets its own Durable Object instance
/// — isolated SQLite, queue, alarm and data. The tenant comes from the
/// `X-Catapulte-Tenant` header (default `"default"`). One tenant per customer /
/// app keeps data separated and spreads load across DO instances. The sender
/// *domain* (A) is orthogonal: a tenant may send from any CF-verified domain.
#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let tenant = req
        .headers()
        .get(TENANT_HEADER)
        .ok()
        .flatten()
        .filter(|t| !t.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_TENANT.to_owned());
    let stub = env
        .durable_object(DO_BINDING)?
        .id_from_name(&tenant)?
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

/// Event publisher that records to the DO's SQLite **and** (best-effort) POSTs
/// each lifecycle event to a configured webhook (`CATAPULTE_WEBHOOK_URL`, with
/// optional `CATAPULTE_WEBHOOK_TOKEN` bearer). A down webhook never blocks email
/// processing — the record always happens, the POST is fire-and-forget.
struct EventSink {
    recorder: DoStore,
    webhook_url: Option<String>,
    webhook_token: Option<String>,
}

impl EventSink {
    fn new(recorder: DoStore, env: &Env) -> Self {
        let var = |name: &str| {
            env.var(name)
                .ok()
                .map(|v| v.to_string())
                .filter(|v| !v.trim().is_empty())
        };
        Self {
            recorder,
            webhook_url: var(WEBHOOK_URL_VAR),
            webhook_token: var(WEBHOOK_TOKEN_VAR),
        }
    }
}

impl EventPublisher for EventSink {
    fn publish(
        &self,
        event: &LifecycleEvent,
    ) -> impl std::future::Future<Output = std::result::Result<(), EventPublisherError>> + Send {
        // DoStore::publish runs the insert eagerly and returns a ready future,
        // so the event is recorded synchronously here; only the webhook awaits.
        let recorded = self.recorder.publish(event);
        let payload = serde_json::json!({
            "event_type": event.event_type(),
            "email_id": event.email_id().as_uuid().to_string(),
            "payload": event.payload(),
        })
        .to_string();
        let url = self.webhook_url.clone();
        let token = self.webhook_token.clone();
        worker::send::SendFuture::new(async move {
            recorded.await?;
            if let Some(url) = url {
                let _ = post_webhook(&url, token.as_deref(), &payload).await;
            }
            Ok(())
        })
    }
}

async fn post_webhook(url: &str, token: Option<&str>, body: &str) -> Result<()> {
    let headers = Headers::new();
    headers.set("content-type", "application/json")?;
    if let Some(t) = token {
        headers.set("authorization", &format!("Bearer {t}"))?;
    }
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(JsValue::from_str(body)));
    let req = Request::new_with_init(url, &init)?;
    Fetch::Request(req).send().await?;
    Ok(())
}

/// Fetches remote attachment URLs via `worker::Fetch` (used by the submit
/// use-case for URL-referenced attachments; uploaded/base64 ones go straight
/// to the store).
struct WorkerAttachmentFetcher;
impl AttachmentFetcher for WorkerAttachmentFetcher {
    fn fetch(
        &self,
        url: &url::Url,
    ) -> impl std::future::Future<Output = std::result::Result<AttachmentReader, AttachmentFetchError>>
    + Send {
        let url = url.clone();
        worker::send::SendFuture::new(async move {
            let mut resp = worker::Fetch::Url(url)
                .send()
                .await
                .map_err(|e| AttachmentFetchError::Fetch {
                    source: anyhow::anyhow!("fetch: {e}"),
                })?;
            if resp.status_code() != 200 {
                return Err(AttachmentFetchError::Fetch {
                    source: anyhow::anyhow!("HTTP {}", resp.status_code()),
                });
            }
            let bytes = resp.bytes().await.map_err(|e| AttachmentFetchError::Fetch {
                source: anyhow::anyhow!("read: {e}"),
            })?;
            let reader: AttachmentReader = Box::pin(std::io::Cursor::new(bytes));
            Ok(reader)
        })
    }
}

// --- app state (the real HttpServerState) -----------------------------------

type SubmitSvc =
    SubmitEmailService<DoStore, DoStore, EventSink, R2AttachmentStore, WorkerAttachmentFetcher>;

#[derive(Clone)]
struct AppState {
    submit_email: Arc<SubmitSvc>,
    list_emails: Arc<ListEmailsService<DoStore>>,
    list_events: Arc<ListEventsService<DoStore>>,
    list_senders: Arc<ListSendersService<DoStore, WasmClock>>,
    check_readiness: Arc<CheckReadinessService<DoStore>>,
}

impl AppState {
    /// Builds the state over the DO's SQLite + the R2 attachment bucket. Each
    /// service gets its own `DoStore` handle (all point at the same database).
    fn new(storage: &Storage, env: &Env) -> Self {
        Self {
            submit_email: Arc::new(SubmitEmailService::new(
                DoStore::new(storage.sql()),
                DoStore::new(storage.sql()),
                EventSink::new(DoStore::new(storage.sql()), env),
                R2AttachmentStore::new(env.clone(), ATTACHMENTS_BINDING),
                WorkerAttachmentFetcher,
            )),
            list_emails: Arc::new(ListEmailsService::new(DoStore::new(storage.sql()))),
            list_events: Arc::new(ListEventsService::new(DoStore::new(storage.sql()))),
            list_senders: Arc::new(ListSendersService::new(
                parse_senders(env),
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
            AppState::new(&storage, &self.env),
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

        // Events go through the sink so Sent/Failed also fire the webhook.
        let sink = EventSink::new(DoStore::new(self.state.storage().sql()), &self.env);
        let now = i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX);
        for item in store.claim_due(now, 10)? {
            let id = uuid::Uuid::parse_str(&item.email_id).map(EmailId::from);
            match self.deliver(&store, &item.email_id).await {
                // (B) Over quota — postpone without counting an attempt.
                Ok(Outcome::Deferred(run_at)) => store.defer(&item.email_id, run_at)?,
                Ok(Outcome::Sent(sender_name)) => {
                    store.set_status(&item.email_id, "sent")?;
                    store.dequeue(&item.email_id)?;
                    if let Ok(id) = id {
                        let _ = sink
                            .publish(&LifecycleEvent::Sent {
                                id,
                                sender_name: sender_name
                                    .unwrap_or_else(|| SenderName::new("cloudflare")),
                                correlation_id: None,
                            })
                            .await;
                    }
                    // GC: an email's attachments are dead once it's terminal.
                    self.gc_attachments(&store, &item.email_id).await;
                }
                Err(e) if item.attempts + 1 >= MAX_ATTEMPTS => {
                    store.set_status(&item.email_id, "failed")?;
                    store.dequeue(&item.email_id)?;
                    if let Ok(id) = id {
                        let _ = sink
                            .publish(&LifecycleEvent::Failed {
                                id,
                                attempt: u32::try_from(item.attempts + 1).unwrap_or(u32::MAX),
                                reason: e.to_string(),
                                error_class: ErrorClass::Delivery,
                                sender_name: None,
                                correlation_id: None,
                            })
                            .await;
                    }
                    self.gc_attachments(&store, &item.email_id).await;
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
    /// Deletes a terminal email's attachment blobs from R2 (immediate GC — once
    /// delivered or permanently failed the blobs are dead). Best-effort: errors
    /// are ignored so they never block the queue.
    async fn gc_attachments(&self, store: &DoStore, email_id: &str) {
        let Ok(Some(email)) = store.load_envelope(email_id) else {
            return;
        };
        if email.attachments.is_empty() {
            return;
        }
        let r2 = R2AttachmentStore::new(self.env.clone(), ATTACHMENTS_BINDING);
        for att in &email.attachments {
            let _ = r2.delete(&att.blob).await;
        }
    }

    /// Renders the email and sends it via the Email Service binding — one
    /// message per recipient. The sender domain must be verified in CF.
    ///
    /// (B) Resolves the configured sender for the from-domain, and if it has a
    /// quota that's exhausted in the window, returns `Deferred` instead of
    /// sending. Returns the matched sender name so the alarm attributes events.
    async fn deliver(&self, store: &DoStore, email_id: &str) -> Result<Outcome> {
        let Some(email) = store
            .load_envelope(email_id)
            .map_err(|e| worker::Error::RustError(e.to_string()))?
        else {
            return Ok(Outcome::Sent(None));
        };

        // (B) Sender resolution + quota enforcement.
        let configs = parse_senders(&self.env);
        let matched = match_sender(&configs, &email.sender).cloned();
        let sender_name = matched.as_ref().map(|c| c.name.clone());
        if let Some(cfg) = &matched
            && let Some(quota) = &cfg.quota
        {
            let now = i64::try_from(Date::now().as_millis()).unwrap_or(i64::MAX);
            let since = quota.range.since_ms(now);
            let stats = store
                .get_stats(std::slice::from_ref(&cfg.name), since)
                .await
                .map_err(|e| worker::Error::RustError(e.to_string()))?;
            let sent = stats.first().map_or(0, |s| s.sent_in_range);
            if sent >= quota.count {
                // Re-check at most hourly; the window slides as old sends age out.
                let window = (now - since).min(3_600_000).max(60_000);
                return Ok(Outcome::Deferred(now + window));
            }
        }

        let (text, html) = self
            .render(&email.body, &email.variables)
            .await
            .map_err(worker::Error::RustError)?;
        let subject = email.subject.as_deref().unwrap_or("");

        // Pull attachment bytes from R2 and base64-encode them for the MIME.
        let mut parts: Vec<MimeAttachment> = Vec::with_capacity(email.attachments.len());
        if !email.attachments.is_empty() {
            let store = R2AttachmentStore::new(self.env.clone(), ATTACHMENTS_BINDING);
            for att in &email.attachments {
                let bytes = store
                    .load_bytes(&att.blob.key)
                    .await
                    .map_err(worker::Error::RustError)?;
                parts.push(MimeAttachment {
                    filename: att.filename.clone(),
                    content_type: att.content_type.clone(),
                    base64: base64::engine::general_purpose::STANDARD.encode(bytes),
                });
            }
        }

        let sender = self.env.send_email(EMAIL_BINDING)?;
        for (_, to) in &email.recipients {
            let raw = build_mime(
                &email.sender,
                to,
                subject,
                text.as_deref(),
                html.as_deref(),
                &parts,
            );
            let message = EmailMessage::new(&email.sender, to, &raw)?;
            sender.send(&message).await?;
        }
        Ok(Outcome::Sent(sender_name))
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
                Ok((None, Some(render_mjml(&interpolate(src, vars)?, &self.env).await?)))
            }
            BodySource::Mjml(MjmlSource::Named(name)) => {
                let src = self.fetch_named_template(name).await?;
                Ok((None, Some(render_mjml(&interpolate(&src, vars)?, &self.env).await?)))
            }
            BodySource::Mjml(MjmlSource::Remote(url)) => {
                let auth = resolver_auth(&self.env, url);
                let src = fetch_remote_template(url, auth.as_deref()).await?;
                Ok((None, Some(render_mjml(&interpolate(&src, vars)?, &self.env).await?)))
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

async fn fetch_remote_template(
    url: &url::Url,
    auth: Option<&str>,
) -> std::result::Result<String, String> {
    // With a per-host auth header (RESOLVER), send a Request carrying it;
    // otherwise a plain GET.
    let mut resp = if let Some(auth) = auth {
        let headers = Headers::new();
        headers
            .set("authorization", auth)
            .map_err(|e| format!("building auth header: {e}"))?;
        let mut init = RequestInit::new();
        init.with_method(Method::Get).with_headers(headers);
        let req = Request::new_with_init(url.as_str(), &init)
            .map_err(|e| format!("building template request: {e}"))?;
        Fetch::Request(req)
            .send()
            .await
            .map_err(|e| format!("fetching remote template: {e}"))?
    } else {
        Fetch::Url(url.clone())
            .send()
            .await
            .map_err(|e| format!("fetching remote template: {e}"))?
    };
    if resp.status_code() != 200 {
        return Err(format!("remote template returned HTTP {}", resp.status_code()));
    }
    resp.text()
        .await
        .map_err(|e| format!("reading remote template: {e}"))
}

/// Per-host auth for remote template fetches (RESOLVER). `CATAPULTE_RESOLVER_AUTH`
/// is a JSON object mapping host → full Authorization header value, e.g.
/// `{"templates.acme.com":"Bearer xyz"}`. Returns the header for the URL's host.
fn resolver_auth(env: &Env, url: &url::Url) -> Option<String> {
    let raw = env.var(RESOLVER_AUTH_VAR).ok()?.to_string();
    if raw.trim().is_empty() {
        return None;
    }
    let map: std::collections::HashMap<String, String> = serde_json::from_str(&raw).ok()?;
    map.get(url.host_str()?).cloned()
}

fn interpolate(
    template: &str,
    vars: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<String, String> {
    let env = minijinja::Environment::new();
    env.render_str(template, minijinja::Value::from_serialize(vars))
        .map_err(|e| format!("interpolation failed: {e}"))
}

async fn render_mjml(mjml: &str, env: &Env) -> std::result::Result<String, String> {
    // Async parse so <mj-include> partials resolve: a URL path via Fetch, any
    // other path as a named partial from the TEMPLATES R2 bucket.
    let opts = Arc::new(mrml::prelude::parser::AsyncParserOptions {
        include_loader: Box::new(WorkerIncludeLoader::new(env.clone())),
    });
    let parsed = mrml::async_parse_with_options(mjml, opts)
        .await
        .map_err(|e| format!("mjml parse failed: {e}"))?;
    parsed
        .element
        .render(&mrml::prelude::render::RenderOptions::default())
        .map_err(|e| format!("mjml render failed: {e}"))
}

/// Resolves `<mj-include>` partials two ways (a "multi" loader):
/// - `path` is an `http(s)` URL  → fetched via `worker::Fetch` (remote partials)
/// - any other `path`            → `<path>.mjml` from the `TEMPLATES` R2 bucket
///
/// mrml's async loader is `?Send` on wasm, so the `!Send` Fetch/R2 futures are
/// fine. Holds the `Env` (behind `SendWrapper`) to reach the R2 binding.
struct WorkerIncludeLoader {
    env: SendWrapper<Rc<Env>>,
}

impl WorkerIncludeLoader {
    fn new(env: Env) -> Self {
        Self {
            env: SendWrapper::new(Rc::new(env)),
        }
    }
}

impl std::fmt::Debug for WorkerIncludeLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerIncludeLoader")
    }
}

#[async_trait::async_trait(?Send)]
impl mrml::prelude::parser::loader::AsyncIncludeLoader for WorkerIncludeLoader {
    async fn async_resolve(
        &self,
        path: &str,
    ) -> std::result::Result<String, mrml::prelude::parser::loader::IncludeLoaderError> {
        use mrml::prelude::parser::loader::IncludeLoaderError;
        let nf = || IncludeLoaderError::not_found(path);

        if path.starts_with("http://") || path.starts_with("https://") {
            let url = url::Url::parse(path).map_err(|_| nf())?;
            let mut resp = Fetch::Url(url).send().await.map_err(|_| nf())?;
            if resp.status_code() != 200 {
                return Err(nf());
            }
            return resp.text().await.map_err(|_| nf());
        }

        // Named partial from R2 (TEMPLATES). `<mj-include path="header">` → header.mjml
        let key = if path.ends_with(".mjml") {
            path.to_owned()
        } else {
            format!("{path}.mjml")
        };
        let bucket = self.env.bucket(TEMPLATES_BINDING).map_err(|_| nf())?;
        let object = bucket.get(key).execute().await.map_err(|_| nf())?.ok_or_else(nf)?;
        object.body().ok_or_else(nf)?.text().await.map_err(|_| nf())
    }
}

/// Outcome of a delivery attempt.
enum Outcome {
    /// Sent via the matched sender (name attributed to lifecycle events).
    Sent(Option<SenderName>),
    /// Over quota — postponed to the given `run_at_ms` without an attempt bump.
    Deferred(i64),
}

// --- (B) sender config + routing -------------------------------------------

/// Sender config as supplied in the `CATAPULTE_SENDERS` env var (JSON array).
#[derive(serde::Deserialize)]
struct SenderConfigDto {
    name: String,
    #[serde(default)]
    match_domain: Option<String>,
    #[serde(default)]
    quota_count: Option<u64>,
    #[serde(default)]
    quota_range: Option<String>,
}

/// Parses `CATAPULTE_SENDERS` (a JSON array) into sender configs. On CF there
/// is one egress (the Email Service binding), so a "sender" is a from-domain
/// with an optional send quota — not an SMTP relay. Empty/absent → no senders.
fn parse_senders(env: &Env) -> Vec<SenderConfig> {
    let Ok(raw) = env.var("CATAPULTE_SENDERS") else {
        return Vec::new();
    };
    let raw = raw.to_string();
    if raw.trim().is_empty() {
        return Vec::new();
    }
    let dtos: Vec<SenderConfigDto> = serde_json::from_str(&raw).unwrap_or_default();
    dtos.into_iter()
        .map(|d| SenderConfig {
            name: SenderName::new(d.name),
            match_sender_domain: d.match_domain,
            quota: d.quota_count.map(|count| SenderQuota {
                count,
                range: parse_range(d.quota_range.as_deref()),
            }),
        })
        .collect()
}

fn parse_range(s: Option<&str>) -> QuotaRange {
    match s {
        Some("hourly") => QuotaRange::Hourly,
        Some("weekly") => QuotaRange::Weekly,
        Some("monthly") => QuotaRange::Monthly,
        _ => QuotaRange::Daily,
    }
}

/// Picks the sender whose `match_domain` equals the from-address domain, else a
/// catch-all sender (one with no `match_domain`), else `None`.
fn match_sender<'a>(configs: &'a [SenderConfig], sender_addr: &str) -> Option<&'a SenderConfig> {
    let domain = sender_addr.rsplit('@').next().unwrap_or("");
    configs
        .iter()
        .find(|c| c.match_sender_domain.as_deref() == Some(domain))
        .or_else(|| configs.iter().find(|c| c.match_sender_domain.is_none()))
}

/// A rendered, base64-encoded attachment ready for the MIME body.
struct MimeAttachment {
    filename: String,
    content_type: String,
    base64: String,
}

/// Builds an RFC822 message (CRLF). The body is `multipart/alternative` when
/// both text + html are present; with attachments the whole thing is wrapped in
/// `multipart/mixed`.
fn build_mime(
    from: &str,
    to: &str,
    subject: &str,
    text: Option<&str>,
    html: Option<&str>,
    attachments: &[MimeAttachment],
) -> String {
    let top = format!("From: {from}\r\nTo: {to}\r\nSubject: {subject}\r\nMIME-Version: 1.0\r\n");
    let body_part = build_body_part(text, html);

    if attachments.is_empty() {
        return format!("{top}{body_part}\r\n");
    }

    let mixed = "catapulte-mixed-boundary";
    let mut out = format!("{top}Content-Type: multipart/mixed; boundary=\"{mixed}\"\r\n\r\n");
    out.push_str(&format!("--{mixed}\r\n{body_part}\r\n"));
    for a in attachments {
        out.push_str(&format!(
            "--{mixed}\r\nContent-Type: {}\r\n\
             Content-Disposition: attachment; filename=\"{}\"\r\n\
             Content-Transfer-Encoding: base64\r\n\r\n{}\r\n",
            a.content_type, a.filename, a.base64
        ));
    }
    out.push_str(&format!("--{mixed}--\r\n"));
    out
}

/// The body section as a MIME part (its own Content-Type header + content, no
/// trailing CRLF) — either a `multipart/alternative` or a single part.
fn build_body_part(text: Option<&str>, html: Option<&str>) -> String {
    match (text, html) {
        (Some(t), Some(h)) => {
            let b = "catapulte-alt-boundary";
            format!(
                "Content-Type: multipart/alternative; boundary=\"{b}\"\r\n\r\n\
                 --{b}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{t}\r\n\
                 --{b}\r\nContent-Type: text/html; charset=utf-8\r\n\r\n{h}\r\n--{b}--"
            )
        }
        (None, Some(h)) => format!("Content-Type: text/html; charset=utf-8\r\n\r\n{h}"),
        (Some(t), None) => format!("Content-Type: text/plain; charset=utf-8\r\n\r\n{t}"),
        (None, None) => "Content-Type: text/plain; charset=utf-8\r\n\r\n".to_owned(),
    }
}

/// Exponential backoff in ms, capped at 5 minutes.
fn backoff_ms(attempts: i64) -> i64 {
    let shift = attempts.clamp(0, 8);
    (1000_i64 << shift).min(300_000)
}
