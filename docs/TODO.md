* partial download of large datasets restarts
* optimizer improvements
  * initial space probe from centroid
  * PCA sort
  * axis prioritization
* Support rates in string form as count/time in fractional, small and large
* provide a second way to have the daemon query (within a phase)
* support displays when there is more than one active phase
* generalize execution shell to make use of wrappers at any node
* support JSON templating authentically
* bring in parser-aware yaml parsing for bare literals vs quotes, retaining 
  Value types
* allow mixed-cursor usage in a scope, as long as the consumption stride 
  aligns for each read point
* Algebraically speaking, an empty partition should not be a problem for 
  iteration, because iterating over an empty set is just doing nothing, 
  and starting and stopping a deamon thread is meaningless if done for as 
  little time as possible.
* "daemon no-spam after a for-loop halt" smells ... too special
* users should not have to use vectordata to tap into vectdordata hosted 
  datsets with the programmatic access layers
* CQL prepared-statement reuse across phase INSTANCES (test-consistency wart):
  `map_op` prepares once per phase instance, so a phase that runs per tier
  (recall_check / load_increment under `for: p`) re-prepares the same stable
  statement once per tier — ~N prepares of one text over a sweep. Server-cheap
  (it hands back the cached statement) but needless round-trips, and it can
  amplify server-side "prepared statement recreation" churn when a schema
  event evicts between tiers. Fix: cache the prepared handle keyed by
  (session, statement text) so a statement is prepared once per session, not
  once per phase instance. Deferred — low impact, not urgent.
* `max_batch_size` (CQL op field) is a SILENT NO-OP and an SRD-30 contract
  violation. It is accepted by the parser allowlist (parse.rs:1847,
  validation.rs:96) but wired to NO batch logic — batching only triggers on
  the `batch: <rows>` param. 5 workloads set `max_batch_size: 64KB` intending
  byte-bounded batching; none actually batch (they send single prepared
  inserts). Root gap: the unknown-field guard checks allowlist MEMBERSHIP, not
  IMPLEMENTATION, so an allowlisted-but-unimplemented field is silently dropped
  instead of erroring. Fix: implement byte-bounded batching (accumulate bound
  prepared rows until the encoded batch would exceed max_batch_size, then
  flush) for both cassandra-cpp and scylla; and/or make the guard reject any
  allowlisted field with no reader so this class can't recur. Until then a
  bare row count (`batch: N`) is the only working batch control.
