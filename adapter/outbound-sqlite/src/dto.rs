//! Wire DTOs now live in `catapulte-outbound-sql-core` so the sqlite, D1 and
//! Durable-Object backends share one canonical format. Re-exported here so the
//! rest of this crate keeps using `crate::dto::*` unchanged.
pub use catapulte_outbound_sql_core::*;
