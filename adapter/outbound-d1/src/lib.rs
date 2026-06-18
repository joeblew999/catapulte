//! Cloudflare D1 implementation of the email storage ports.
//!
//! This is the Workers-native counterpart to `outbound-sqlite`. D1 is reached
//! through the Workers binding (not a SQL wire protocol), so this adapter uses
//! the `worker` crate rather than `sqlx`, and stores ids/JSON as TEXT columns
//! (D1 blob round-tripping is awkward; a fresh CF schema doesn't need it).
//!
//! The domain ports are declared `Send + Sync`, but the D1 handle and its
//! futures are `!Send` (they hold `JsValue`). `SendWrapper` bridges that —
//! sound because Workers run single-threaded. This is the central friction of
//! running catapulte on Workers; it is isolated to the CF adapters.
//!
//! Slice 1 scope: `save` / `list_emails` / `delete` / `set_attachments` /
//! `list_all_attachment_blobs` against the `emails` table. Delivery-event
//! status (the `lifecycle_events` join) and the recipient/template filters are
//! left for a later slice — `list_emails` reports every row as `Queued`.

use std::future::Future;
use std::rc::Rc;

use catapulte_domain::entity::attachment::{AttachmentRef, BlobRef};
use catapulte_domain::entity::body::{BodySource, MjmlSource};
use catapulte_domain::entity::email::{EmailId, RecipientKind};
use catapulte_domain::entity::envelope::Envelope;
use catapulte_domain::port::email_repository::{
    EmailRecord, EmailRepository, EmailRepositoryError, EmailStatus, ListEmailsParams, SaveResult,
};
use send_wrapper::SendWrapper;
use serde::{Deserialize, Serialize};
use worker::D1Database;
use worker::send::SendFuture;
use worker::wasm_bindgen::JsValue;

// --- wire DTOs (the JSON shape stored in TEXT columns) -----------------------
// Mirrors `catapulte-outbound-sqlite::dto`; the domain entities don't derive
// Serialize, so each storage adapter owns its own wire format.

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BodySourceDto {
    Plain { text: Option<String>, html: Option<String> },
    MjmlInline { source: String },
    MjmlNamed { name: String },
    MjmlRemote { url: String },
}

impl From<&BodySource> for BodySourceDto {
    fn from(body: &BodySource) -> Self {
        match body {
            BodySource::Plain(p) => Self::Plain {
                text: p.text().map(str::to_owned),
                html: p.html().map(str::to_owned),
            },
            BodySource::Mjml(MjmlSource::Inline(s)) => Self::MjmlInline { source: s.clone() },
            BodySource::Mjml(MjmlSource::Named(n)) => Self::MjmlNamed { name: n.clone() },
            BodySource::Mjml(MjmlSource::Remote(u)) => Self::MjmlRemote { url: u.to_string() },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecipientKindDto {
    To,
    Cc,
    Bcc,
}

impl From<RecipientKind> for RecipientKindDto {
    fn from(kind: RecipientKind) -> Self {
        match kind {
            RecipientKind::To => Self::To,
            RecipientKind::Cc => Self::Cc,
            RecipientKind::Bcc => Self::Bcc,
        }
    }
}

impl From<RecipientKindDto> for RecipientKind {
    fn from(dto: RecipientKindDto) -> Self {
        match dto {
            RecipientKindDto::To => Self::To,
            RecipientKindDto::Cc => Self::Cc,
            RecipientKindDto::Bcc => Self::Bcc,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RecipientDto {
    kind: RecipientKindDto,
    address: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlobRefDto {
    backend: String,
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AttachmentRefDto {
    filename: String,
    content_type: String,
    size_bytes: u64,
    blob: BlobRefDto,
}

impl From<&AttachmentRef> for AttachmentRefDto {
    fn from(a: &AttachmentRef) -> Self {
        Self {
            filename: a.filename.clone(),
            content_type: a.content_type.clone(),
            size_bytes: a.size_bytes,
            blob: BlobRefDto {
                backend: a.blob.backend.clone(),
                key: a.blob.key.clone(),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EnvelopeBodyDto {
    source: BodySourceDto,
    attachments: Vec<AttachmentRefDto>,
}

fn recipients_to_dto(recipients: &[(RecipientKind, String)]) -> Vec<RecipientDto> {
    recipients
        .iter()
        .map(|(k, a)| RecipientDto {
            kind: (*k).into(),
            address: a.clone(),
        })
        .collect()
}

// --- row shapes D1 deserializes into -----------------------------------------

#[derive(Deserialize)]
struct EmailRow {
    id: String,
    idempotency_key: Option<String>,
    subject: Option<String>,
    sender: String,
    recipients: String,
    created_at_ms: f64,
}

#[derive(Deserialize)]
struct IdRow {
    id: String,
}

#[derive(Deserialize)]
struct BodyRow {
    body: String,
}

// --- adapter -----------------------------------------------------------------

/// Email storage backed by a Cloudflare D1 database binding.
///
/// `D1Database` is neither `Send`/`Sync` nor `Clone`, so it is held in an
/// `Rc` behind a `SendWrapper` (which is unconditionally `Send + Sync`). Each
/// port method clones the `Rc` into its future and wraps that future in
/// `SendFuture` to satisfy the `Send` bound the domain ports require.
#[derive(Clone)]
pub struct D1Adapter {
    db: SendWrapper<Rc<D1Database>>,
}

impl D1Adapter {
    /// Wraps a D1 binding handle obtained from the Worker `Env`.
    #[must_use]
    pub fn new(db: D1Database) -> Self {
        Self {
            db: SendWrapper::new(Rc::new(db)),
        }
    }
}

fn storage<E: std::fmt::Display>(ctx: &str, e: E) -> EmailRepositoryError {
    EmailRepositoryError::Storage {
        source: anyhow::anyhow!("{ctx}: {e}"),
    }
}

fn opt_str(v: Option<&str>) -> JsValue {
    match v {
        Some(s) => JsValue::from_str(s),
        None => JsValue::NULL,
    }
}

impl EmailRepository for D1Adapter {
    fn save(
        &self,
        id: EmailId,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<SaveResult, EmailRepositoryError>> + Send {
        let db = (*self.db).clone();
        let id_str = id.as_uuid().to_string();
        let idempotency_key = envelope.idempotency_key.clone();
        let correlation_id = envelope.correlation_id.clone();
        let subject = envelope.subject.clone();
        let sender = envelope.sender.clone();
        let recipients_json = serde_json::to_string(&recipients_to_dto(&envelope.recipients));
        let body_json = serde_json::to_string(&EnvelopeBodyDto {
            source: BodySourceDto::from(&envelope.body),
            attachments: envelope.attachments.iter().map(AttachmentRefDto::from).collect(),
        });
        let variables_json = serde_json::to_string(&envelope.variables);

        SendFuture::new(async move {
            let recipients_json = recipients_json.map_err(|e| storage("encoding recipients", e))?;
            let body_json = body_json.map_err(|e| storage("encoding body", e))?;
            let variables_json = variables_json.map_err(|e| storage("encoding variables", e))?;

            let insert_sql = "INSERT OR IGNORE INTO emails \
                (id, idempotency_key, correlation_id, subject, sender, recipients, body, variables) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            let stmt = db
                .prepare(insert_sql)
                .bind(&[
                    JsValue::from_str(&id_str),
                    opt_str(idempotency_key.as_deref()),
                    opt_str(correlation_id.as_deref()),
                    opt_str(subject.as_deref()),
                    JsValue::from_str(&sender),
                    JsValue::from_str(&recipients_json),
                    JsValue::from_str(&body_json),
                    JsValue::from_str(&variables_json),
                ])
                .map_err(|e| storage("binding insert", e))?;
            stmt.run().await.map_err(|e| storage("inserting email", e))?;

            // Without an idempotency key, a zero-row insert can only be an
            // unexpected primary-key collision — surface it as an error, like
            // the sqlite adapter does.
            let Some(key) = idempotency_key.as_deref() else {
                let existing = db
                    .prepare("SELECT id FROM emails WHERE id = ?")
                    .bind(&[JsValue::from_str(&id_str)])
                    .map_err(|e| storage("binding id check", e))?
                    .first::<IdRow>(None)
                    .await
                    .map_err(|e| storage("checking inserted id", e))?;
                return match existing {
                    Some(_) => Ok(SaveResult::Created(id)),
                    None => Err(storage(
                        "insert skipped",
                        "no idempotency key (unexpected id collision)",
                    )),
                };
            };

            // With a key, the canonical row is whatever now owns that key.
            let existing = db
                .prepare("SELECT id FROM emails WHERE idempotency_key = ?")
                .bind(&[JsValue::from_str(key)])
                .map_err(|e| storage("binding idempotency lookup", e))?
                .first::<IdRow>(None)
                .await
                .map_err(|e| storage("fetching existing email by idempotency key", e))?
                .ok_or_else(|| storage("idempotency lookup", "row vanished after insert"))?;

            let existing_uuid =
                uuid::Uuid::parse_str(&existing.id).map_err(|e| storage("parsing existing id", e))?;
            if existing_uuid == id.as_uuid() {
                Ok(SaveResult::Created(id))
            } else {
                Ok(SaveResult::Duplicate(EmailId::from(existing_uuid)))
            }
        })
    }

    fn list_emails(
        &self,
        params: ListEmailsParams,
    ) -> impl Future<Output = Result<Vec<EmailRecord>, EmailRepositoryError>> + Send {
        let db = (*self.db).clone();
        SendFuture::new(async move {
            let mut sql = String::from(
                "SELECT id, idempotency_key, subject, sender, recipients, created_at_ms \
                 FROM emails WHERE 1=1",
            );
            let mut binds: Vec<JsValue> = Vec::new();
            if let Some(id) = params.id {
                sql.push_str(" AND id = ?");
                binds.push(JsValue::from_str(&id.as_uuid().to_string()));
            }
            if let Some(after) = params.after_ms {
                sql.push_str(" AND created_at_ms > ?");
                binds.push(JsValue::from_f64(after as f64));
            }
            if let Some(before) = params.before_ms {
                sql.push_str(" AND created_at_ms < ?");
                binds.push(JsValue::from_f64(before as f64));
            }
            sql.push_str(" ORDER BY created_at_ms DESC, id DESC LIMIT ? OFFSET ?");
            binds.push(JsValue::from_f64(f64::from(params.limit)));
            binds.push(JsValue::from_f64(f64::from(params.offset)));

            let result = db
                .prepare(&sql)
                .bind(&binds)
                .map_err(|e| storage("binding list query", e))?
                .all()
                .await
                .map_err(|e| storage("listing emails", e))?;
            let rows: Vec<EmailRow> = result.results().map_err(|e| storage("decoding rows", e))?;

            rows.into_iter()
                .map(|row| {
                    let uuid = uuid::Uuid::parse_str(&row.id)
                        .map_err(|e| storage("parsing row id", e))?;
                    let recipients: Vec<RecipientDto> = serde_json::from_str(&row.recipients)
                        .map_err(|e| storage("decoding recipients", e))?;
                    Ok(EmailRecord {
                        id: EmailId::from(uuid),
                        idempotency_key: row.idempotency_key,
                        subject: row.subject,
                        sender: row.sender,
                        recipients: recipients
                            .into_iter()
                            .map(|r| (r.kind.into(), r.address))
                            .collect(),
                        created_at_ms: row.created_at_ms as i64,
                        status: EmailStatus::Queued,
                    })
                })
                .collect()
        })
    }

    fn set_attachments(
        &self,
        id: EmailId,
        attachments: &[AttachmentRef],
    ) -> impl Future<Output = Result<(), EmailRepositoryError>> + Send {
        let db = (*self.db).clone();
        let id_str = id.as_uuid().to_string();
        let new_dtos: Vec<AttachmentRefDto> = attachments.iter().map(AttachmentRefDto::from).collect();
        SendFuture::new(async move {
            let existing = db
                .prepare("SELECT body FROM emails WHERE id = ?")
                .bind(&[JsValue::from_str(&id_str)])
                .map_err(|e| storage("binding body read", e))?
                .first::<BodyRow>(None)
                .await
                .map_err(|e| storage("reading body for set_attachments", e))?
                .ok_or_else(|| storage("set_attachments", "email not found"))?;

            let mut body: EnvelopeBodyDto = serde_json::from_str(&existing.body)
                .map_err(|e| storage("decoding existing body", e))?;
            body.attachments = new_dtos;
            let body_json = serde_json::to_string(&body).map_err(|e| storage("encoding body", e))?;

            db.prepare("UPDATE emails SET body = ? WHERE id = ?")
                .bind(&[JsValue::from_str(&body_json), JsValue::from_str(&id_str)])
                .map_err(|e| storage("binding body update", e))?
                .run()
                .await
                .map_err(|e| storage("writing body for set_attachments", e))?;
            Ok(())
        })
    }

    fn delete(
        &self,
        id: EmailId,
    ) -> impl Future<Output = Result<(), EmailRepositoryError>> + Send {
        let db = (*self.db).clone();
        let id_str = id.as_uuid().to_string();
        SendFuture::new(async move {
            db.prepare("DELETE FROM emails WHERE id = ?")
                .bind(&[JsValue::from_str(&id_str)])
                .map_err(|e| storage("binding delete", e))?
                .run()
                .await
                .map_err(|e| storage("deleting email", e))?;
            Ok(())
        })
    }

    fn list_all_attachment_blobs(
        &self,
    ) -> impl Future<Output = Result<Vec<BlobRef>, EmailRepositoryError>> + Send {
        let db = (*self.db).clone();
        SendFuture::new(async move {
            let result = db
                .prepare("SELECT body FROM emails")
                .all()
                .await
                .map_err(|e| storage("listing bodies", e))?;
            let rows: Vec<BodyRow> = result.results().map_err(|e| storage("decoding bodies", e))?;
            let mut blobs = Vec::new();
            for row in rows {
                if let Ok(body) = serde_json::from_str::<EnvelopeBodyDto>(&row.body) {
                    for att in body.attachments {
                        blobs.push(BlobRef {
                            backend: att.blob.backend,
                            key: att.blob.key,
                        });
                    }
                }
            }
            Ok(blobs)
        })
    }
}
