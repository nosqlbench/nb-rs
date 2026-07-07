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
* SRD-30 guard hardening: the unknown-field guard checks allowlist MEMBERSHIP,
  not IMPLEMENTATION, so an allowlisted-but-unimplemented op field is silently
  dropped rather than erroring — this is how `max_batch_size` stayed a silent
  no-op for so long (now implemented, byte-bounded batching + server-limit
  reads, via SRD-103). Harden the guard to reject any allowlisted field that
  has no reader, so this class of silent no-op can't recur.
