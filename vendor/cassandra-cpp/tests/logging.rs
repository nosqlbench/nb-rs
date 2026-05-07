mod help;

#[cfg(any(feature = "slog", feature = "log"))]
use cassandra_cpp::*;

#[cfg(feature = "slog")]
use slog::*;
use std::sync::Arc;
use std::sync::Mutex;

/// `set_slog_logger` and `set_log_logger` both replace the
/// driver's global logger; `logtest::Logger::start()` likewise
/// installs a process-wide log subscriber. The three tests in
/// this file all touch that singleton — running them in parallel
/// (cargo's default) lets one test's logger replace another's
/// before the captured-log assertions run, producing flaky
/// `pop().unwrap()` panics. Take this lock around any test that
/// installs a logger.
static LOGGING_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Simple drain which accumulates all messages written to it.
#[derive(Clone)]
struct MyDrain(Arc<Mutex<String>>);

impl Default for MyDrain {
    fn default() -> Self {
        MyDrain(Arc::new(Mutex::new("".to_string())))
    }
}

#[cfg(feature = "slog")]
impl Drain for MyDrain {
    type Ok = ();
    type Err = ();

    fn log(
        &self,
        record: &Record,
        _values: &OwnedKVList,
    ) -> ::std::result::Result<Self::Ok, Self::Err> {
        self.0
            .lock()
            .unwrap()
            .push_str(&format!("{}", record.msg()));
        Ok(())
    }
}

#[cfg(feature = "slog")]
#[tokio::test]
async fn test_slog_logger() {
    let _guard = LOGGING_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let drain = MyDrain::default();
    let logger = Logger::root(drain.clone().fuse(), o!());

    set_level(LogLevel::WARN);
    set_slog_logger(logger);

    let mut cluster = Cluster::default();
    cluster
        .set_contact_points("absolute-gibberish.invalid")
        .unwrap();
    cluster.connect().await.expect_err("Should fail to connect");

    let log_output: String = drain.0.lock().unwrap().clone();
    assert!(
        log_output.contains("Unable to resolve address for absolute-gibberish.invalid"),
        "{}",
        log_output
    );
}

#[cfg(feature = "log")]
#[tokio::test]
async fn test_log_logger() {
    use log::Level;

    let _guard = LOGGING_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut logger = logtest::Logger::start();
    set_level(LogLevel::WARN);
    set_log_logger();

    let mut cluster = Cluster::default();
    cluster
        .set_contact_points("absolute-gibberish.invalid")
        .unwrap();
    cluster.connect().await.expect_err("Should fail to connect");

    // `logtest::Logger` is a *process-global* `log` crate
    // subscriber. By the time the cassandra-cpp resolver
    // emits its address-resolution failure, the queue
    // also contains unrelated records from any other
    // thread that logged via the `log` crate while this
    // test was running — typically tokio / mio internals
    // (`registering event source with poller …`) but
    // potentially anything in the binary that uses
    // `log::*`. Popping the head and asserting on it is
    // therefore order-dependent and flaky under
    // workspace-level `cargo test` parallelism.
    //
    // Drain the queue and look for the cassandra-cpp
    // record specifically (matched by its distinctive
    // resolver target). Fail with a useful message if it
    // never arrives within a generous bound; soft-ignore
    // every other record because they're all unrelated to
    // this test's contract.
    const RESOLVER_TARGET: &str =
        "{anonymous}::DefaultClusterMetadataResolver::on_resolve";
    const MAX_DRAIN: usize = 1024;

    let mut seen_records: Vec<String> = Vec::new();
    let mut found: Option<logtest::Record> = None;
    for _ in 0..MAX_DRAIN {
        match logger.pop() {
            Some(record) if record.target() == RESOLVER_TARGET => {
                found = Some(record);
                break;
            }
            Some(record) => {
                seen_records.push(format!(
                    "  - target={:?} level={:?} args={:?}",
                    record.target(), record.level(), record.args(),
                ));
                continue;
            }
            None => break,
        }
    }
    let record = found.unwrap_or_else(|| panic!(
        "expected a `log` record with target {RESOLVER_TARGET:?} \
         after the failed connect, but never saw one. Drained {} \
         unrelated record(s):\n{}",
        seen_records.len(),
        seen_records.join("\n"),
    ));

    assert_eq!(
        record.args(),
        "Unable to resolve address for absolute-gibberish.invalid:9042\n",
    );
    assert_eq!(record.level(), Level::Error);
    assert_eq!(record.target(), RESOLVER_TARGET);
    assert_eq!(record.key_values(), vec!());
}

#[tokio::test]
async fn test_metrics() {
    // This is just a check that metrics work and actually notice requests.
    // Need to send a cassandra query that will produce a positive number for the min_us metric
    // (minimum time to respond to a request in microseconds), i.e. a request that make cassandra
    // take more than 1 microsecond to respond to. Do a couple of setup queries, with IF NOT EXISTS
    // so that they don't fail if this test is repeated.
    let query1 = "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = {
                      'class' : 'SimpleStrategy',
                      'replication_factor' : 1
                      };";
    let query2 = "CREATE TABLE IF NOT EXISTS cycling.cyclist_name (
                       id UUID PRIMARY KEY,
                       lastname text,
                       firstname text );"; //create table
    let query3 = "INSERT INTO cycling.cyclist_name (id, lastname, firstname)
                       VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK','Steven')
                       USING TTL 86400 AND TIMESTAMP 123456789;";

    let session = help::create_test_session().await;
    session.execute(&query1).await.unwrap();
    session.execute(&query2).await.unwrap();
    session.execute(&query3).await.unwrap();

    let metrics = session.get_metrics();
    assert_eq!(metrics.total_connections, 1);
    assert!(metrics.min_us > 0);
}
