//! Cloudflare R2 implementation of the `AttachmentStore` port.
//!
//! Workers-native attachment storage: blobs live in an R2 bucket keyed by a
//! generated UUID. Like the D1 adapter, the R2 `Bucket` handle and its futures
//! are `!Send`, so they are bridged to the `Send + Sync` port via
//! `SendWrapper`/`SendFuture` (sound on the single-threaded Workers runtime).

use std::future::Future;
use std::rc::Rc;

use catapulte_domain::entity::attachment::BlobRef;
use catapulte_domain::port::attachment_store::{
    AttachmentReader, AttachmentStore, AttachmentStoreError, PutResult,
};
use send_wrapper::SendWrapper;
use tokio::io::AsyncReadExt;
use worker::Env;
use worker::send::SendFuture;

const BACKEND: &str = "r2";

/// Attachment storage backed by a Cloudflare R2 bucket binding.
///
/// Holds the `Env` and resolves the bucket lazily per operation, so requests
/// that touch no attachments work even when the bucket isn't bound — only
/// actual attachment I/O requires the binding.
#[derive(Clone)]
pub struct R2AttachmentStore {
    env: SendWrapper<Rc<Env>>,
    binding: String,
}

impl R2AttachmentStore {
    #[must_use]
    pub fn new(env: Env, binding: impl Into<String>) -> Self {
        Self {
            env: SendWrapper::new(Rc::new(env)),
            binding: binding.into(),
        }
    }

    /// Reads a blob's bytes directly (used by the delivery path to build MIME).
    ///
    /// # Errors
    /// Returns an error if the binding/object is missing or the read fails.
    pub async fn load_bytes(&self, key: &str) -> Result<Vec<u8>, String> {
        let bucket = self.env.bucket(&self.binding).map_err(|e| e.to_string())?;
        let object = bucket
            .get(key.to_owned())
            .execute()
            .await
            .map_err(|e| format!("r2 get {key}: {e}"))?
            .ok_or_else(|| format!("attachment blob not found: {key}"))?;
        object
            .body()
            .ok_or_else(|| format!("attachment blob {key} has no body"))?
            .bytes()
            .await
            .map_err(|e| format!("reading attachment {key}: {e}"))
    }
}

fn io(ctx: &str, e: impl std::fmt::Display) -> AttachmentStoreError {
    AttachmentStoreError::Io {
        source: anyhow::anyhow!("{ctx}: {e}"),
    }
}

impl AttachmentStore for R2AttachmentStore {
    fn put(
        &self,
        mut reader: AttachmentReader,
    ) -> impl Future<Output = Result<PutResult, AttachmentStoreError>> + Send {
        let env = self.env.clone();
        let binding = self.binding.clone();
        SendFuture::new(async move {
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .await
                .map_err(|e| io("reading attachment", e))?;
            let size_bytes = bytes.len() as u64;
            let key = uuid::Uuid::new_v4().to_string();
            let bucket = env.bucket(&binding).map_err(|e| io("r2 binding", e))?;
            bucket
                .put(key.clone(), bytes)
                .execute()
                .await
                .map_err(|e| io("r2 put", e))?;
            Ok(PutResult {
                blob: BlobRef {
                    backend: BACKEND.to_owned(),
                    key,
                },
                size_bytes,
            })
        })
    }

    fn get(
        &self,
        blob: &BlobRef,
    ) -> impl Future<Output = Result<AttachmentReader, AttachmentStoreError>> + Send {
        let env = self.env.clone();
        let binding = self.binding.clone();
        let key = blob.key.clone();
        SendFuture::new(async move {
            let bucket = env.bucket(&binding).map_err(|e| io("r2 binding", e))?;
            let object = bucket
                .get(key)
                .execute()
                .await
                .map_err(|e| io("r2 get", e))?
                .ok_or(AttachmentStoreError::NotFound)?;
            let bytes = object
                .body()
                .ok_or(AttachmentStoreError::NotFound)?
                .bytes()
                .await
                .map_err(|e| io("r2 read", e))?;
            let reader: AttachmentReader = Box::pin(std::io::Cursor::new(bytes));
            Ok(reader)
        })
    }

    fn delete(
        &self,
        blob: &BlobRef,
    ) -> impl Future<Output = Result<(), AttachmentStoreError>> + Send {
        let env = self.env.clone();
        let binding = self.binding.clone();
        let key = blob.key.clone();
        SendFuture::new(async move {
            let bucket = env.bucket(&binding).map_err(|e| io("r2 binding", e))?;
            bucket.delete(key).await.map_err(|e| io("r2 delete", e))?;
            Ok(())
        })
    }
}
