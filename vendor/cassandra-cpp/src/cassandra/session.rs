#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(missing_copy_implementations)]

use crate::cassandra::custom_payload::CustomPayloadResponse;
use crate::cassandra::error::*;
use crate::cassandra::future::CassFuture;
use crate::cassandra::metrics::SessionMetrics;
use crate::cassandra::prepared::PreparedStatement;
use crate::cassandra::result::CassResult;
use crate::cassandra::schema::schema_meta::SchemaMeta;
use crate::cassandra::statement::Statement;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::{cassandra::batch::Batch, BatchType};

use crate::cassandra_sys::cass_session_close;
use crate::cassandra_sys::cass_session_execute;
use crate::cassandra_sys::cass_session_execute_batch;
use crate::cassandra_sys::cass_session_free;
use crate::cassandra_sys::cass_session_get_metrics;
use crate::cassandra_sys::cass_session_get_schema_meta;
use crate::cassandra_sys::cass_session_new;
use crate::cassandra_sys::cass_session_prepare_n;
use crate::cassandra_sys::CassSession as _Session;

use std::mem;
use std::os::raw::c_char;
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq)]
pub struct SessionInner(*mut _Session);

// The underlying C type has no thread-local state, and explicitly supports access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for SessionInner {}
unsafe impl Sync for SessionInner {}

impl SessionInner {
    fn new(inner: *mut _Session) -> Arc<Self> {
        Arc::new(Self(inner))
    }
}

/// A session object is used to execute queries and maintains cluster state through
/// the control connection. The control connection is used to auto-discover nodes and
/// monitor cluster changes (topology and schema). Each session also maintains multiple
/// pools of connections to cluster nodes which are used to query the cluster.
///
/// Instances of the session object are thread-safe to execute queries.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Session(pub Arc<SessionInner>);

impl ProtectedInner<*mut _Session> for SessionInner {
    fn inner(&self) -> *mut _Session {
        self.0
    }
}

impl ProtectedInner<*mut _Session> for Session {
    fn inner(&self) -> *mut _Session {
        self.0.inner()
    }
}

impl Protected<*mut _Session> for Session {
    fn build(inner: *mut _Session) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Session(SessionInner::new(inner))
    }
}

impl Drop for SessionInner {
    /// Frees a session instance. If the session is still connected it will be synchronously
    /// closed before being deallocated.
    fn drop(&mut self) {
        unsafe { cass_session_free(self.0) }
    }
}

impl Default for Session {
    fn default() -> Session {
        Session::new()
    }
}

impl Session {
    pub(crate) fn new() -> Session {
        unsafe { Session(SessionInner::new(cass_session_new())) }
    }

    /// Closes the session connection and synchronously waits
    /// for any in-flight requests to complete before the
    /// returned future resolves.
    ///
    /// Returns a `CassFuture<()>` that resolves once the
    /// underlying C++ driver has finished its close handshake.
    /// Callers can compose timeouts (`tokio::time::timeout`)
    /// around the await to bound how long they'll wait for a
    /// hung session.
    ///
    /// `Drop for SessionInner` still calls `cass_session_free`
    /// to deallocate; calling `close().await` first is the
    /// graceful path. A session that's dropped without
    /// awaiting `close()` is closed synchronously by the C++
    /// driver inside `cass_session_free`, which can block for
    /// up to the underlying close timeout.
    pub fn close(&self) -> CassFuture<()> {
        let inner_future = unsafe { cass_session_close(self.inner()) };
        <CassFuture<()>>::build(self.clone(), inner_future)
    }

    /// Create a prepared statement with the given query.
    pub async fn prepare(&self, query: impl AsRef<str>) -> Result<PreparedStatement> {
        let query = query.as_ref();
        let prepare_future = {
            let query_ptr = query.as_ptr() as *const c_char;
            CassFuture::build(self.clone(), unsafe {
                cass_session_prepare_n(self.inner(), query_ptr, query.len())
            })
        };
        prepare_future.await
    }

    /// Creates a statement with the given query.
    pub fn statement(&self, query: impl AsRef<str>) -> Statement {
        let query = query.as_ref();
        let param_count = query.matches('?').count();
        Statement::new(self.clone(), query, param_count)
    }

    /// Execute a batch statement.
    pub fn execute_batch(&self, batch: &Batch) -> CassFuture<CassResult> {
        let inner_future = unsafe { cass_session_execute_batch(self.inner(), batch.inner()) };
        <CassFuture<CassResult>>::build(self.clone(), inner_future)
    }

    /// Execute a batch statement and get any custom payloads from the response.
    pub fn execute_batch_with_payloads(
        &self,
        batch: &Batch,
    ) -> CassFuture<(CassResult, CustomPayloadResponse)> {
        let inner_future = unsafe { cass_session_execute_batch(self.inner(), batch.inner()) };
        <CassFuture<(CassResult, CustomPayloadResponse)>>::build(self.clone(), inner_future)
    }

    /// Execute a batch statement and retrieve the server-side
    /// trace UUID from the future. The UUID is `Some` when at
    /// least one statement in the batch had `set_tracing(true)`
    /// set before being added (the cpp-driver returns
    /// `CASS_ERROR_LIB_NO_TRACING_ID` otherwise, surfaced here
    /// as `None`).
    ///
    /// Mirrors [`Session::execute_with_tracing`] in shape — for
    /// batched execution where we want the trace UUID alongside
    /// the result, without awaiting in the constructor.
    pub fn execute_batch_with_tracing(
        &self,
        batch: &Batch,
    ) -> CassFuture<(CassResult, Option<crate::cassandra::uuid::Uuid>)> {
        let inner_future = unsafe { cass_session_execute_batch(self.inner(), batch.inner()) };
        <CassFuture<(CassResult, Option<crate::cassandra::uuid::Uuid>)>>::build(
            self.clone(), inner_future,
        )
    }

    /// Executes a given query.
    pub async fn execute(&self, query: impl AsRef<str>) -> Result<CassResult> {
        let statement = self.statement(query);
        statement.execute().await
    }

    /// Creates a new batch that is bound to this session.
    pub fn batch(&self, batch_type: BatchType) -> Batch {
        Batch::new(batch_type, self.clone())
    }

    /// Execute a statement and get any custom payloads from the response.
    pub fn execute_with_payloads(
        &self,
        statement: &Statement,
    ) -> CassFuture<(CassResult, CustomPayloadResponse)> {
        let inner_future = unsafe { cass_session_execute(self.inner(), statement.inner()) };
        <CassFuture<(CassResult, CustomPayloadResponse)>>::build(self.clone(), inner_future)
    }

    /// Execute a statement and retrieve the server-side trace
    /// UUID from the future. The UUID is `Some` when the
    /// statement had `set_tracing(true)` set before execute
    /// (the cpp-driver returns `CASS_ERROR_LIB_NO_TRACING_ID`
    /// otherwise, surfaced here as `None`). Pair with
    /// `system_traces.sessions` / `system_traces.events`
    /// queries by trace UUID for full client-driven trace
    /// capture.
    ///
    /// Mirrors [`Session::execute_with_payloads`] in shape —
    /// borrows the statement (so it can be re-used or dropped
    /// by the caller) and returns the future without awaiting,
    /// matching the pattern callers expect for the
    /// auxiliary-data execute variants.
    pub fn execute_with_tracing(
        &self,
        statement: &Statement,
    ) -> CassFuture<(CassResult, Option<crate::cassandra::uuid::Uuid>)> {
        let inner_future = unsafe { cass_session_execute(self.inner(), statement.inner()) };
        <CassFuture<(CassResult, Option<crate::cassandra::uuid::Uuid>)>>::build(
            self.clone(), inner_future,
        )
    }

    /// Gets a snapshot of this session's schema metadata. The returned
    /// snapshot of the schema metadata is not updated. This function
    /// must be called again to retrieve any schema changes since the
    /// previous call.
    pub fn get_schema_meta(&self) -> SchemaMeta {
        unsafe { SchemaMeta::build(cass_session_get_schema_meta(self.inner())) }
    }

    /// Gets a copy of this session's performance/diagnostic metrics.
    pub fn get_metrics(&self) -> SessionMetrics {
        unsafe {
            let mut metrics = mem::zeroed();
            cass_session_get_metrics(self.inner(), &mut metrics);
            SessionMetrics::build(&metrics)
        }
    }

    //    pub fn get_schema(&self) -> Schema {
    //        unsafe { Schema(cass_session_get_schema(self.0)) }
    //    }
}
