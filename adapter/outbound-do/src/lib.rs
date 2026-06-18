//! Durable Object SQLite storage + queue for catapulte.
//!
//! A single primitive replaces D1 + CF Queues + cron:
//! - the DO's embedded SQLite holds `emails` and `email_queue`,
//! - the DO `alarm()` drives the delivery queue (see the `worker` crate),
//! - the DO's serialized single-threaded execution removes the row-claim race
//!   that the native sqlite backend's `claimed_until` column exists to handle.
//!
//! `SqlStorage` is `Send + Sync` and its `exec` is synchronous (local SQLite),
//! so — unlike the D1 adapter — no `SendWrapper`/`SendFuture` bridging is
//! needed: the port futures have no `!Send` await points.
//!
//! Wire DTOs are shared with the sqlite/D1 backends via
//! `catapulte-outbound-sql-core`.

use std::future::Future;

use catapulte_domain::entity::attachment::{AttachmentRef, BlobRef};
use catapulte_domain::entity::body::BodySource;
use catapulte_domain::entity::email::{EmailId, RecipientKind};
use catapulte_domain::entity::envelope::Envelope;
use catapulte_domain::entity::lifecycle_event::LifecycleEvent;
use catapulte_domain::entity::sender::SenderName;
use catapulte_domain::port::email_queue::{AckToken, DequeuedEmail, EmailQueue, EmailQueueError};
use catapulte_domain::port::event_publisher::{EventPublisher, EventPublisherError};
use catapulte_domain::port::email_repository::{
    EmailRecord, EmailRepository, EmailRepositoryError, EmailStatus, ListEmailsParams, SaveResult,
};
use catapulte_domain::port::event_repository::{
    EventRecord, EventRepository, EventRepositoryError, ListEventsParams,
};
use catapulte_domain::port::health::{HealthCheck, HealthCheckError};
use catapulte_domain::port::sender_usage::{SenderStats, SenderUsage, SenderUsageError};
use catapulte_outbound_sql_core::{
    AttachmentRefDto, BodySourceDto, EnvelopeBodyDto, EnvelopeBodyDtoDeser, RecipientDto,
    recipients_from_dto, recipients_to_dto,
};
use serde::Deserialize;
use worker::{Result as WResult, SqlStorage, SqlStorageValue};

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS emails (
    id              TEXT PRIMARY KEY NOT NULL,
    idempotency_key TEXT,
    correlation_id  TEXT,
    subject         TEXT,
    sender          TEXT NOT NULL,
    recipients      TEXT NOT NULL,
    body            TEXT NOT NULL,
    variables       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    created_at_ms   INTEGER NOT NULL DEFAULT (CAST(unixepoch('now','subsec')*1000 AS INTEGER))
);
CREATE UNIQUE INDEX IF NOT EXISTS emails_idempotency_key
    ON emails(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS email_queue (
    email_id   TEXT PRIMARY KEY NOT NULL REFERENCES emails(id),
    run_at_ms  INTEGER NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS email_queue_run_at ON email_queue(run_at_ms);
CREATE TABLE IF NOT EXISTS lifecycle_events (
    id            TEXT PRIMARY KEY NOT NULL,
    email_id      TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       TEXT,
    sender_name   TEXT,
    error_class   TEXT,
    created_at_ms INTEGER NOT NULL DEFAULT (CAST(unixepoch('now','subsec')*1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS lifecycle_events_email ON lifecycle_events(email_id, created_at_ms);
";

// --- row shapes -------------------------------------------------------------

#[derive(Deserialize)]
struct EmailRow {
    id: String,
    idempotency_key: Option<String>,
    subject: Option<String>,
    sender: String,
    recipients: String,
    status: String,
    created_at_ms: f64,
}

fn status_from_str(s: &str) -> EmailStatus {
    match s {
        "sent" => EmailStatus::Sent,
        "failed" => EmailStatus::Failed,
        _ => EmailStatus::Queued,
    }
}

#[derive(Deserialize)]
struct IdRow {
    id: String,
}

#[derive(Deserialize)]
struct BodyRow {
    body: String,
}

#[derive(Deserialize)]
struct RunAtRow {
    run_at_ms: f64,
}

/// A queue entry that is due for a delivery attempt.
#[derive(Debug, Deserialize)]
pub struct DueEmail {
    pub email_id: String,
    pub attempts: i64,
}

/// Everything the delivery step needs for one email, with domain types restored.
pub struct StoredEmail {
    pub sender: String,
    pub subject: Option<String>,
    pub recipients: Vec<(RecipientKind, String)>,
    pub body: BodySource,
    pub variables: serde_json::Map<String, serde_json::Value>,
    pub attachments: Vec<AttachmentRef>,
}

#[derive(Deserialize)]
struct FullRow {
    sender: String,
    subject: Option<String>,
    recipients: String,
    body: String,
    variables: String,
}

// --- adapter ----------------------------------------------------------------

/// Storage + queue backed by a Durable Object's embedded SQLite.
pub struct DoStore {
    sql: SqlStorage,
}

fn err<E: std::fmt::Display>(ctx: &str, e: E) -> EmailRepositoryError {
    EmailRepositoryError::Storage {
        source: anyhow::anyhow!("{ctx}: {e}"),
    }
}

impl DoStore {
    #[must_use]
    pub fn new(sql: SqlStorage) -> Self {
        Self { sql }
    }

    /// Idempotently creates the schema. Cheap to call on every DO activation.
    ///
    /// # Errors
    /// Returns an error if the DDL fails to execute.
    pub fn init_schema(&self) -> WResult<()> {
        self.sql.exec(SCHEMA, None)?;
        Ok(())
    }

    // --- queue ops (the bit that replaces CF Queues) ------------------------

    /// Enqueues an email for delivery at `run_at_ms`.
    ///
    /// # Errors
    /// Returns an error if the insert fails.
    pub fn enqueue(&self, id: EmailId, run_at_ms: i64) -> WResult<()> {
        self.sql.exec(
            "INSERT OR REPLACE INTO email_queue (email_id, run_at_ms, attempts) \
             VALUES (?, ?, COALESCE((SELECT attempts FROM email_queue WHERE email_id = ?), 0))",
            vec![
                id.as_uuid().to_string().into(),
                SqlStorageValue::Integer(run_at_ms),
                id.as_uuid().to_string().into(),
            ],
        )?;
        Ok(())
    }

    /// Returns up to `limit` queue entries whose `run_at_ms <= now_ms`.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub fn claim_due(&self, now_ms: i64, limit: u32) -> WResult<Vec<DueEmail>> {
        self.sql
            .exec(
                "SELECT email_id, attempts FROM email_queue \
                 WHERE run_at_ms <= ? ORDER BY run_at_ms LIMIT ?",
                vec![
                    SqlStorageValue::Integer(now_ms),
                    SqlStorageValue::Integer(i64::from(limit)),
                ],
            )?
            .to_array()
    }

    /// Reschedules an entry for a later retry, bumping its attempt count.
    ///
    /// # Errors
    /// Returns an error if the update fails.
    pub fn reschedule(&self, email_id: &str, run_at_ms: i64) -> WResult<()> {
        self.sql.exec(
            "UPDATE email_queue SET run_at_ms = ?, attempts = attempts + 1 WHERE email_id = ?",
            vec![SqlStorageValue::Integer(run_at_ms), email_id.into()],
        )?;
        Ok(())
    }

    /// Removes an entry from the queue (delivered or permanently failed).
    ///
    /// # Errors
    /// Returns an error if the delete fails.
    pub fn dequeue(&self, email_id: &str) -> WResult<()> {
        self.sql.exec(
            "DELETE FROM email_queue WHERE email_id = ?",
            vec![email_id.into()],
        )?;
        Ok(())
    }

    /// Marks an email's terminal delivery status (`"sent"` / `"failed"`).
    ///
    /// # Errors
    /// Returns an error if the update fails.
    pub fn set_status(&self, email_id: &str, status: &str) -> WResult<()> {
        self.sql.exec(
            "UPDATE emails SET status = ? WHERE id = ?",
            vec![status.into(), email_id.into()],
        )?;
        Ok(())
    }

    /// Loads the full envelope for a delivery attempt, with domain types
    /// restored. `None` if the row is gone.
    ///
    /// # Errors
    /// Returns an error if the query or decoding fails.
    pub fn load_envelope(&self, email_id: &str) -> Result<Option<StoredEmail>, EmailRepositoryError> {
        let rows: Vec<FullRow> = self
            .sql
            .exec(
                "SELECT sender, subject, recipients, body, variables FROM emails WHERE id = ?",
                vec![email_id.into()],
            )
            .map_err(|e| err("loading envelope", e))?
            .to_array()
            .map_err(|e| err("decoding envelope", e))?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };

        let recipients: Vec<RecipientDto> =
            serde_json::from_str(&row.recipients).map_err(|e| err("decoding recipients", e))?;
        let deser: EnvelopeBodyDtoDeser =
            serde_json::from_str(&row.body).map_err(|e| err("decoding body", e))?;
        let (source_dto, attachment_dtos) = deser.split();
        let body = BodySource::try_from(source_dto).map_err(|e| err("rebuilding body", e))?;
        let attachments = attachment_dtos.into_iter().map(AttachmentRef::from).collect();
        let variables: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&row.variables).unwrap_or_default();

        Ok(Some(StoredEmail {
            sender: row.sender,
            subject: row.subject,
            recipients: recipients_from_dto(recipients),
            body,
            variables,
            attachments,
        }))
    }

    /// Earliest pending `run_at_ms`, for arming the next alarm. `None` if empty.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub fn next_run_at(&self) -> WResult<Option<i64>> {
        let rows: Vec<RunAtRow> = self
            .sql
            .exec("SELECT run_at_ms FROM email_queue ORDER BY run_at_ms LIMIT 1", None)?
            .to_array()?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(rows.first().map(|r| r.run_at_ms as i64))
    }
}

impl EmailRepository for DoStore {
    fn save(
        &self,
        id: EmailId,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<SaveResult, EmailRepositoryError>> + Send {
        // exec is synchronous; do the work eagerly, return a ready result.
        let result = self.save_sync(id, envelope);
        async move { result }
    }

    fn list_emails(
        &self,
        params: ListEmailsParams,
    ) -> impl Future<Output = Result<Vec<EmailRecord>, EmailRepositoryError>> + Send {
        let result = self.list_emails_sync(&params);
        async move { result }
    }

    fn set_attachments(
        &self,
        id: EmailId,
        attachments: &[AttachmentRef],
    ) -> impl Future<Output = Result<(), EmailRepositoryError>> + Send {
        let result = self.set_attachments_sync(id, attachments);
        async move { result }
    }

    fn delete(
        &self,
        id: EmailId,
    ) -> impl Future<Output = Result<(), EmailRepositoryError>> + Send {
        let result = self
            .sql
            .exec(
                "DELETE FROM emails WHERE id = ?",
                vec![id.as_uuid().to_string().into()],
            )
            .map(|_| ())
            .map_err(|e| err("deleting email", e));
        async move { result }
    }

    fn list_all_attachment_blobs(
        &self,
    ) -> impl Future<Output = Result<Vec<BlobRef>, EmailRepositoryError>> + Send {
        let result = self.list_blobs_sync();
        async move { result }
    }
}

impl DoStore {
    fn save_sync(&self, id: EmailId, envelope: &Envelope) -> Result<SaveResult, EmailRepositoryError> {
        let id_str = id.as_uuid().to_string();
        let recipients_json = serde_json::to_string(&recipients_to_dto(&envelope.recipients))
            .map_err(|e| err("encoding recipients", e))?;
        let body_json = serde_json::to_string(&EnvelopeBodyDto {
            source: BodySourceDto::from(&envelope.body),
            attachments: envelope.attachments.iter().map(AttachmentRefDto::from).collect(),
        })
        .map_err(|e| err("encoding body", e))?;
        let variables_json =
            serde_json::to_string(&envelope.variables).map_err(|e| err("encoding variables", e))?;

        self.sql
            .exec(
                "INSERT OR IGNORE INTO emails \
                 (id, idempotency_key, correlation_id, subject, sender, recipients, body, variables) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                vec![
                    id_str.clone().into(),
                    envelope.idempotency_key.clone().into(),
                    envelope.correlation_id.clone().into(),
                    envelope.subject.clone().into(),
                    envelope.sender.clone().into(),
                    recipients_json.into(),
                    body_json.into(),
                    variables_json.into(),
                ],
            )
            .map_err(|e| err("inserting email", e))?;

        let Some(key) = envelope.idempotency_key.as_deref() else {
            return Ok(SaveResult::Created(id));
        };

        let existing: Vec<IdRow> = self
            .sql
            .exec(
                "SELECT id FROM emails WHERE idempotency_key = ?",
                vec![key.into()],
            )
            .map_err(|e| err("idempotency lookup", e))?
            .to_array()
            .map_err(|e| err("decoding idempotency lookup", e))?;
        let existing = existing
            .into_iter()
            .next()
            .ok_or_else(|| err("idempotency lookup", "row vanished after insert"))?;
        let existing_uuid =
            uuid::Uuid::parse_str(&existing.id).map_err(|e| err("parsing existing id", e))?;
        if existing_uuid == id.as_uuid() {
            Ok(SaveResult::Created(id))
        } else {
            Ok(SaveResult::Duplicate(EmailId::from(existing_uuid)))
        }
    }

    fn list_emails_sync(
        &self,
        params: &ListEmailsParams,
    ) -> Result<Vec<EmailRecord>, EmailRepositoryError> {
        let mut sql = String::from(
            "SELECT id, idempotency_key, subject, sender, recipients, status, created_at_ms \
             FROM emails WHERE 1=1",
        );
        let mut binds: Vec<SqlStorageValue> = Vec::new();
        if let Some(id) = params.id {
            sql.push_str(" AND id = ?");
            binds.push(id.as_uuid().to_string().into());
        }
        if let Some(after) = params.after_ms {
            sql.push_str(" AND created_at_ms > ?");
            binds.push(SqlStorageValue::Integer(after));
        }
        if let Some(before) = params.before_ms {
            sql.push_str(" AND created_at_ms < ?");
            binds.push(SqlStorageValue::Integer(before));
        }
        if let Some(status) = params.status {
            sql.push_str(" AND status = ?");
            binds.push(
                match status {
                    EmailStatus::Sent => "sent",
                    EmailStatus::Failed => "failed",
                    EmailStatus::Queued => "queued",
                }
                .into(),
            );
        }
        sql.push_str(" ORDER BY created_at_ms DESC, id DESC LIMIT ? OFFSET ?");
        binds.push(SqlStorageValue::Integer(i64::from(params.limit)));
        binds.push(SqlStorageValue::Integer(i64::from(params.offset)));

        let rows: Vec<EmailRow> = self
            .sql
            .exec(&sql, binds)
            .map_err(|e| err("listing emails", e))?
            .to_array()
            .map_err(|e| err("decoding rows", e))?;

        rows.into_iter()
            .map(|row| {
                let uuid =
                    uuid::Uuid::parse_str(&row.id).map_err(|e| err("parsing row id", e))?;
                let recipients: Vec<RecipientDto> = serde_json::from_str(&row.recipients)
                    .map_err(|e| err("decoding recipients", e))?;
                #[allow(clippy::cast_possible_truncation)]
                Ok(EmailRecord {
                    id: EmailId::from(uuid),
                    idempotency_key: row.idempotency_key,
                    subject: row.subject,
                    sender: row.sender,
                    recipients: recipients_from_dto(recipients),
                    created_at_ms: row.created_at_ms as i64,
                    status: status_from_str(&row.status),
                })
            })
            .collect()
    }

    fn set_attachments_sync(
        &self,
        id: EmailId,
        attachments: &[AttachmentRef],
    ) -> Result<(), EmailRepositoryError> {
        let id_str = id.as_uuid().to_string();
        let rows: Vec<BodyRow> = self
            .sql
            .exec(
                "SELECT body FROM emails WHERE id = ?",
                vec![id_str.clone().into()],
            )
            .map_err(|e| err("reading body", e))?
            .to_array()
            .map_err(|e| err("decoding body", e))?;
        let existing = rows
            .into_iter()
            .next()
            .ok_or_else(|| err("set_attachments", "email not found"))?;

        let deser: EnvelopeBodyDtoDeser =
            serde_json::from_str(&existing.body).map_err(|e| err("decoding existing body", e))?;
        let (source, _) = deser.split();
        let body = EnvelopeBodyDto {
            source,
            attachments: attachments.iter().map(AttachmentRefDto::from).collect(),
        };
        let body_json = serde_json::to_string(&body).map_err(|e| err("encoding body", e))?;

        self.sql
            .exec(
                "UPDATE emails SET body = ? WHERE id = ?",
                vec![body_json.into(), id_str.into()],
            )
            .map_err(|e| err("writing body", e))?;
        Ok(())
    }

    fn list_blobs_sync(&self) -> Result<Vec<BlobRef>, EmailRepositoryError> {
        let rows: Vec<BodyRow> = self
            .sql
            .exec("SELECT body FROM emails", None)
            .map_err(|e| err("listing bodies", e))?
            .to_array()
            .map_err(|e| err("decoding bodies", e))?;
        let mut blobs = Vec::new();
        for row in rows {
            if let Ok(deser) = serde_json::from_str::<EnvelopeBodyDtoDeser>(&row.body) {
                let (_, attachments) = deser.split();
                for att in attachments {
                    blobs.push(BlobRef {
                        backend: att.blob.backend,
                        key: att.blob.key,
                    });
                }
            }
        }
        Ok(blobs)
    }
}

// --- additional ports the HTTP use-cases need --------------------------------
// Submit goes through SubmitEmailService (repo + queue + event-publisher);
// processing stays in the DO alarm (inherent claim_due/dequeue), so the queue
// port only needs a real `enqueue`. Events aren't tracked yet (status lives in
// the emails table), so EventRepository is empty and SenderUsage is zero.

impl EmailQueue for DoStore {
    fn enqueue(
        &self,
        id: EmailId,
        _envelope: &Envelope,
    ) -> impl Future<Output = Result<(), EmailQueueError>> + Send {
        // run_at_ms = 0 → immediately due; the DO alarm picks it up.
        let r = self
            .sql
            .exec(
                "INSERT OR REPLACE INTO email_queue (email_id, run_at_ms, attempts) VALUES (?, 0, 0)",
                vec![id.as_uuid().to_string().into()],
            )
            .map(|_| ())
            .map_err(|e| EmailQueueError::Storage {
                source: anyhow::anyhow!("enqueue: {e}"),
            });
        async move { r }
    }

    fn dequeue(&self) -> impl Future<Output = Result<DequeuedEmail, EmailQueueError>> + Send {
        // The DO drains the queue via its alarm (inherent claim_due), not this
        // blocking port method.
        async {
            Err(EmailQueueError::Storage {
                source: anyhow::anyhow!("dequeue unsupported; the DO alarm drains the queue"),
            })
        }
    }

    fn ack(&self, _token: AckToken) -> impl Future<Output = Result<(), EmailQueueError>> + Send {
        async { Ok(()) }
    }

    fn nack(
        &self,
        _token: AckToken,
        _delay: std::time::Duration,
    ) -> impl Future<Output = Result<(), EmailQueueError>> + Send {
        async { Ok(()) }
    }
}

impl EventPublisher for DoStore {
    fn publish(
        &self,
        event: &LifecycleEvent,
    ) -> impl Future<Output = Result<(), EventPublisherError>> + Send {
        let r = self
            .sql
            .exec(
                "INSERT INTO lifecycle_events \
                 (id, email_id, event_type, payload, sender_name, error_class) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    uuid::Uuid::now_v7().to_string().into(),
                    event.email_id().as_uuid().to_string().into(),
                    event.event_type().to_owned().into(),
                    event.payload().to_string().into(),
                    event.sender_name().map(|s| s.as_str().to_owned()).into(),
                    event.error_class().map(|e| e.as_str().to_owned()).into(),
                ],
            )
            .map(|_| ())
            .map_err(|e| EventPublisherError::Publish {
                source: anyhow::anyhow!("publish: {e}"),
            });
        async move { r }
    }
}

#[derive(Deserialize)]
struct EventRow {
    id: String,
    email_id: String,
    event_type: String,
    payload: Option<String>,
    sender_name: Option<String>,
    error_class: Option<String>,
    created_at_ms: f64,
}

impl EventRepository for DoStore {
    fn list_events(
        &self,
        params: ListEventsParams,
    ) -> impl Future<Output = Result<Vec<EventRecord>, EventRepositoryError>> + Send {
        let result = self.list_events_sync(&params);
        async move { result }
    }
}

impl DoStore {
    fn list_events_sync(
        &self,
        params: &ListEventsParams,
    ) -> Result<Vec<EventRecord>, EventRepositoryError> {
        let ev_err = |ctx: &str, e: String| EventRepositoryError::Storage {
            source: anyhow::anyhow!("{ctx}: {e}"),
        };
        let mut sql = String::from(
            "SELECT id, email_id, event_type, payload, sender_name, error_class, created_at_ms \
             FROM lifecycle_events WHERE 1=1",
        );
        let mut binds: Vec<SqlStorageValue> = Vec::new();
        if let Some(email_id) = params.email_id {
            sql.push_str(" AND email_id = ?");
            binds.push(email_id.as_uuid().to_string().into());
        }
        if let Some(event_type) = &params.event_type {
            sql.push_str(" AND event_type = ?");
            binds.push(event_type.clone().into());
        }
        if let Some(sender_name) = &params.sender_name {
            sql.push_str(" AND sender_name = ?");
            binds.push(sender_name.clone().into());
        }
        if let Some(error_class) = &params.error_class {
            sql.push_str(" AND error_class = ?");
            binds.push(error_class.as_str().to_owned().into());
        }
        if let Some(after) = params.after_ms {
            sql.push_str(" AND created_at_ms > ?");
            binds.push(SqlStorageValue::Integer(after));
        }
        if let Some(before) = params.before_ms {
            sql.push_str(" AND created_at_ms < ?");
            binds.push(SqlStorageValue::Integer(before));
        }
        sql.push_str(" ORDER BY created_at_ms DESC, id DESC LIMIT ? OFFSET ?");
        binds.push(SqlStorageValue::Integer(i64::from(params.limit)));
        binds.push(SqlStorageValue::Integer(i64::from(params.offset)));

        let rows: Vec<EventRow> = self
            .sql
            .exec(&sql, binds)
            .map_err(|e| ev_err("listing events", e.to_string()))?
            .to_array()
            .map_err(|e| ev_err("decoding events", e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let id = uuid::Uuid::parse_str(&row.id)
                    .map_err(|e| ev_err("parsing event id", e.to_string()))?;
                let email_uuid = uuid::Uuid::parse_str(&row.email_id)
                    .map_err(|e| ev_err("parsing email id", e.to_string()))?;
                let payload = row
                    .payload
                    .and_then(|p| serde_json::from_str::<serde_json::Value>(&p).ok());
                #[allow(clippy::cast_possible_truncation)]
                Ok(EventRecord {
                    id,
                    email_id: EmailId::from(email_uuid),
                    event_type: row.event_type,
                    payload,
                    sender_name: row.sender_name.map(SenderName::new),
                    error_class: row.error_class,
                    created_at_ms: row.created_at_ms as i64,
                })
            })
            .collect()
    }
}

impl SenderUsage for DoStore {
    fn get_stats(
        &self,
        names: &[SenderName],
        _since_ms: i64,
    ) -> impl Future<Output = Result<Vec<SenderStats>, SenderUsageError>> + Send {
        let stats: Vec<SenderStats> = names
            .iter()
            .map(|name| SenderStats {
                name: name.clone(),
                sent_in_range: 0,
                failed_in_range: 0,
            })
            .collect();
        async move { Ok(stats) }
    }
}

impl HealthCheck for DoStore {
    fn check(&self) -> impl Future<Output = Result<(), HealthCheckError>> + Send {
        // A reachable DO with its SQLite is ready by construction.
        async { Ok(()) }
    }
}
