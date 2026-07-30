// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Session-level system-performance sampler.
//!
//! Reads `/proc` directly on a fixed interval (default 5 s) and publishes
//! host-side utilization as session-scoped gauges plus a live observer
//! callback for display surfaces. One task per session, spawned by the
//! runner when `--sysmon` / `sysmon=` asks for it — measurement of the
//! machine under the workload, not of the workload.
//!
//! What is measured, and from where:
//!
//! - **Disk** — `/proc/diskstats`, every line. Utilization per device is
//!   `Δio_ticks / Δwall` (io_ticks is the 10th stat field: milliseconds the
//!   device had I/O in flight — the same quantity `iostat -x %util` reports).
//!   Only the HIGHEST device utilization is recorded, with the device named
//!   beside it. On NVMe this saturates as an idle-detector rather than a
//!   capacity meter (dozens of requests run in parallel), which is exactly
//!   what a "is the disk the bottleneck" glance wants.
//! - **CPU** — `/proc/stat`. Two separate measures: the MEAN utilization from
//!   the aggregate `cpu ` line, and the MAXIMUM single-core saturation from
//!   the `cpuN` lines (with the core named). A pinned compaction thread can
//!   saturate one core while the mean reads 3% — both facts matter and
//!   neither substitutes for the other.
//! - **Memory** — `/proc/meminfo`. Two separate measures:
//!   `(MemTotal − MemAvailable) / MemTotal` (committed: what is actually
//!   claimed and not readily reclaimable — the kernel's own estimate) and
//!   `(MemTotal − MemFree) / MemTotal` (everything, page cache included).
//!   On a database host the second sits near 100% by design; the pair reads
//!   as "how much is spoken for" vs "how much is touched".
//! - **Memory bandwidth** — resctrl MBM (`/sys/fs/resctrl/mon_data`), IF
//!   available. Requires the resctrl filesystem mounted and a configured
//!   peak (`sysmon-membw-gbps=`) to turn bytes/s into a utilization. Absent
//!   either, the item is omitted rather than guessed.
//!
//! Counters are cumulative, so every utilization here is a pairwise delta
//! over the sample window; the published gauge is the latest window's value
//! and any windowing beyond that belongs to MetricsQL at query time.

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use nbrs_metrics::component::Component;
use nbrs_metrics::instruments::gauge::ValueGauge;

/// One completed sample window, as handed to display surfaces.
#[derive(Debug, Clone, PartialEq)]
pub struct SysmonSample {
    /// Highest per-device utilization this window, 0..1.
    pub disk_util: f64,
    /// The device that had it.
    pub disk_top: String,
    /// Mean CPU utilization across the machine, 0..1.
    pub cpu_mean: f64,
    /// The single most saturated core's utilization, 0..1.
    pub cpu_max_core: f64,
    /// Which core that was.
    pub cpu_top_core: usize,
    /// (MemTotal − MemAvailable) / MemTotal — claimed memory.
    pub mem_committed: f64,
    /// (MemTotal − MemFree) / MemTotal — everything, page cache included.
    pub mem_cached: f64,
    /// Memory-bandwidth utilization, when resctrl MBM and a configured peak
    /// make it computable. `None` means "not available", never "zero".
    pub membw_util: Option<f64>,
}

/// Sampler configuration, resolved by the runner from session args.
#[derive(Debug, Clone)]
pub struct SysmonConfig {
    /// Sample window. Default 5 s.
    pub interval: Duration,
    /// Peak memory bandwidth in bytes/s, for the resctrl item.
    pub membw_peak_bytes_per_s: Option<f64>,
}

impl Default for SysmonConfig {
    fn default() -> Self {
        Self { interval: Duration::from_secs(5), membw_peak_bytes_per_s: None }
    }
}

// ---------------------------------------------------------------------------
// Pure parsers + delta math. Everything below reads STRINGS so the arithmetic
// is testable on fixtures; only the spawn loop touches the filesystem.
// ---------------------------------------------------------------------------

/// Per-device cumulative `io_ticks` (ms with I/O in flight) from
/// `/proc/diskstats`. Field layout per the kernel's Documentation/iostats:
/// `major minor name <17 stat fields>`; io_ticks is stat field 10, i.e.
/// whitespace token 12.
pub fn parse_diskstats(text: &str) -> Vec<(String, u64)> {
    text.lines()
        .filter_map(|line| {
            let t: Vec<&str> = line.split_whitespace().collect();
            let name = t.get(2)?;
            let io_ticks: u64 = t.get(12)?.parse().ok()?;
            Some((name.to_string(), io_ticks))
        })
        .collect()
}

/// The highest per-device utilization between two diskstats snapshots taken
/// `dt_ms` apart. Devices present in only one snapshot are skipped (hotplug
/// between samples); an empty intersection yields `None`.
pub fn max_disk_util(
    prev: &[(String, u64)],
    cur: &[(String, u64)],
    dt_ms: f64,
) -> Option<(String, f64)> {
    if dt_ms <= 0.0 {
        return None;
    }
    let mut best: Option<(String, f64)> = None;
    for (name, cur_ticks) in cur {
        let Some((_, prev_ticks)) = prev.iter().find(|(n, _)| n == name) else {
            continue;
        };
        let util = (cur_ticks.saturating_sub(*prev_ticks) as f64 / dt_ms)
            .clamp(0.0, 1.0);
        if best.as_ref().is_none_or(|(_, b)| util > *b) {
            best = Some((name.clone(), util));
        }
    }
    best
}

/// Cumulative jiffy counts for one `cpu` line: (busy, total).
///
/// total = user+nice+system+idle+iowait+irq+softirq+steal (the first eight
/// fields; guest time is already accounted inside user/nice). busy = total −
/// idle − iowait: iowait is idle-with-an-excuse, and counting it as busy
/// would read an I/O-bound stall as CPU saturation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuTicks {
    pub busy: u64,
    pub total: u64,
}

/// Aggregate + per-core cumulative ticks from `/proc/stat`. The aggregate
/// `cpu ` line is index `None`-equivalent (returned separately); cores come
/// back in `cpuN` order.
pub fn parse_proc_stat(text: &str) -> Option<(CpuTicks, Vec<CpuTicks>)> {
    let mut aggregate: Option<CpuTicks> = None;
    let mut cores: Vec<(usize, CpuTicks)> = Vec::new();
    for line in text.lines() {
        let mut t = line.split_whitespace();
        let Some(name) = t.next() else { continue };
        if !name.starts_with("cpu") {
            continue;
        }
        let fields: Vec<u64> = t.filter_map(|f| f.parse().ok()).collect();
        if fields.len() < 8 {
            continue;
        }
        let total: u64 = fields[..8].iter().sum();
        let idle = fields[3] + fields[4];
        let ticks = CpuTicks { busy: total - idle, total };
        if name == "cpu" {
            aggregate = Some(ticks);
        } else if let Ok(n) = name[3..].parse::<usize>() {
            cores.push((n, ticks));
        }
    }
    cores.sort_by_key(|(n, _)| *n);
    aggregate.map(|a| (a, cores.into_iter().map(|(_, t)| t).collect()))
}

/// Utilization between two tick snapshots: Δbusy / Δtotal.
pub fn cpu_util(prev: CpuTicks, cur: CpuTicks) -> f64 {
    let dt = cur.total.saturating_sub(prev.total);
    if dt == 0 {
        return 0.0;
    }
    (cur.busy.saturating_sub(prev.busy) as f64 / dt as f64).clamp(0.0, 1.0)
}

/// The most saturated core between two snapshots. Core lists of different
/// lengths (offline/online between samples) compare over the shared prefix.
pub fn max_core_util(prev: &[CpuTicks], cur: &[CpuTicks]) -> Option<(usize, f64)> {
    prev.iter()
        .zip(cur.iter())
        .enumerate()
        .map(|(i, (p, c))| (i, cpu_util(*p, *c)))
        .max_by(|a, b| a.1.total_cmp(&b.1))
}

/// The three `/proc/meminfo` fields the two utilization measures need,
/// in kB as the kernel reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemInfo {
    pub total_kb: u64,
    pub free_kb: u64,
    pub available_kb: u64,
}

pub fn parse_meminfo(text: &str) -> Option<MemInfo> {
    let mut total = None;
    let mut free = None;
    let mut available = None;
    for line in text.lines() {
        let mut t = line.split_whitespace();
        match t.next() {
            Some("MemTotal:") => total = t.next()?.parse().ok(),
            Some("MemFree:") => free = t.next()?.parse().ok(),
            Some("MemAvailable:") => available = t.next()?.parse().ok(),
            _ => {}
        }
    }
    Some(MemInfo {
        total_kb: total?,
        free_kb: free?,
        available_kb: available?,
    })
}

/// The two memory measures: (committed, everything-including-page-cache).
pub fn mem_utils(m: MemInfo) -> (f64, f64) {
    if m.total_kb == 0 {
        return (0.0, 0.0);
    }
    let committed =
        (m.total_kb.saturating_sub(m.available_kb)) as f64 / m.total_kb as f64;
    let cached = (m.total_kb.saturating_sub(m.free_kb)) as f64 / m.total_kb as f64;
    (committed.clamp(0.0, 1.0), cached.clamp(0.0, 1.0))
}

/// Sum of resctrl MBM total-bytes counters across mon_data groups, when the
/// resctrl filesystem is mounted with monitoring. `None` when unavailable —
/// which is the common case (needs a resctrl mount and CPU support).
fn read_membw_bytes() -> Option<u64> {
    let root = std::path::Path::new("/sys/fs/resctrl/mon_data");
    let entries = std::fs::read_dir(root).ok()?;
    let mut sum: u64 = 0;
    let mut seen = false;
    for e in entries.flatten() {
        let f = e.path().join("mbm_total_bytes");
        if let Ok(text) = std::fs::read_to_string(&f)
            && let Ok(v) = text.trim().parse::<u64>()
        {
            sum += v;
            seen = true;
        }
    }
    seen.then_some(sum)
}

// ---------------------------------------------------------------------------
// The sampler task.
// ---------------------------------------------------------------------------

struct Gauges {
    disk: Arc<ValueGauge>,
    cpu_mean: Arc<ValueGauge>,
    cpu_core_max: Arc<ValueGauge>,
    mem_committed: Arc<ValueGauge>,
    mem_cached: Arc<ValueGauge>,
    membw: Option<Arc<ValueGauge>>,
}

/// Register the sysmon gauge family on the session component. Direct
/// registration on the session root — no child component, no new labels: the
/// samples describe the whole host, which is exactly the session's
/// dimensional cell.
fn register_gauges(
    component: &Arc<RwLock<Component>>,
    membw_available: bool,
) -> Result<Gauges, String> {
    let mut guard = component.write().unwrap_or_else(|e| e.into_inner());
    let labels = guard.effective_labels().clone();
    let mut mk = |family: &str| -> Result<Arc<ValueGauge>, String> {
        let g = Arc::new(ValueGauge::new(labels.with("family", family)));
        guard.register_instrument(
            family,
            nbrs_metrics::component::InstrumentRef::Gauge(g.clone()),
        )?;
        Ok(g)
    };
    Ok(Gauges {
        disk: mk("sysmon_disk_util")?,
        cpu_mean: mk("sysmon_cpu_util")?,
        cpu_core_max: mk("sysmon_cpu_core_max")?,
        mem_committed: mk("sysmon_mem_util")?,
        mem_cached: mk("sysmon_mem_util_cached")?,
        membw: if membw_available {
            Some(mk("sysmon_membw_util")?)
        } else {
            None
        },
    })
}

/// Spawn the sampler. Runs until session shutdown; publishes each window to
/// the session gauges and to `observer.sysmon_update`.
pub fn spawn(
    config: SysmonConfig,
    component: Arc<RwLock<Component>>,
    observer: Arc<dyn crate::observer::RunObserver>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    // Probe availability ONCE: the item either exists for the session or it
    // does not. A resctrl mount appearing mid-run is not a case worth
    // chasing samples for.
    let membw_available =
        config.membw_peak_bytes_per_s.is_some() && read_membw_bytes().is_some();
    let gauges = register_gauges(&component, membw_available)?;

    let mut shutdown = crate::session_signals::subscribe_shutdown();
    Ok(tokio::spawn(async move {
        let mut prev_disks = std::fs::read_to_string("/proc/diskstats")
            .map(|t| parse_diskstats(&t))
            .unwrap_or_default();
        let mut prev_cpu = std::fs::read_to_string("/proc/stat")
            .ok()
            .and_then(|t| parse_proc_stat(&t));
        let mut prev_membw = read_membw_bytes();
        let mut prev_at = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = tokio::time::sleep(config.interval) => {}
                _ = shutdown.changed() => break,
            }
            let now = std::time::Instant::now();
            let dt = now.duration_since(prev_at);
            let dt_ms = dt.as_secs_f64() * 1000.0;
            prev_at = now;

            let cur_disks = std::fs::read_to_string("/proc/diskstats")
                .map(|t| parse_diskstats(&t))
                .unwrap_or_default();
            let cur_cpu = std::fs::read_to_string("/proc/stat")
                .ok()
                .and_then(|t| parse_proc_stat(&t));
            let mem = std::fs::read_to_string("/proc/meminfo")
                .ok()
                .and_then(|t| parse_meminfo(&t));

            let (disk_top, disk_util) =
                max_disk_util(&prev_disks, &cur_disks, dt_ms)
                    .unwrap_or_else(|| (String::new(), 0.0));
            prev_disks = cur_disks;

            let (cpu_mean, cpu_top_core, cpu_max_core) =
                match (&prev_cpu, &cur_cpu) {
                    (Some((pa, pc)), Some((ca, cc))) => {
                        let mean = cpu_util(*pa, *ca);
                        let (idx, max) =
                            max_core_util(pc, cc).unwrap_or((0, 0.0));
                        (mean, idx, max)
                    }
                    _ => (0.0, 0, 0.0),
                };
            prev_cpu = cur_cpu;

            let (mem_committed, mem_cached) =
                mem.map(mem_utils).unwrap_or((0.0, 0.0));

            let membw_util = match (&gauges.membw, config.membw_peak_bytes_per_s) {
                (Some(_), Some(peak)) if peak > 0.0 => {
                    let cur = read_membw_bytes();
                    let util = match (prev_membw, cur) {
                        (Some(p), Some(c)) => Some(
                            ((c.saturating_sub(p)) as f64
                                / dt.as_secs_f64().max(1e-9)
                                / peak)
                                .clamp(0.0, 1.0),
                        ),
                        _ => None,
                    };
                    prev_membw = cur;
                    util
                }
                _ => None,
            };

            gauges.disk.set(disk_util);
            gauges.cpu_mean.set(cpu_mean);
            gauges.cpu_core_max.set(cpu_max_core);
            gauges.mem_committed.set(mem_committed);
            gauges.mem_cached.set(mem_cached);
            if let (Some(g), Some(u)) = (&gauges.membw, membw_util) {
                g.set(u);
            }

            observer.sysmon_update(&SysmonSample {
                disk_util,
                disk_top: disk_top.clone(),
                cpu_mean,
                cpu_max_core,
                cpu_top_core,
                mem_committed,
                mem_cached,
                membw_util,
            });
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real lines from this host's /proc/diskstats — io_ticks is token 12.
    #[test]
    fn diskstats_reads_io_ticks_from_the_tenth_stat_field() {
        let text = "\
 259       0 nvme0n1 1083809 77036 79071293 1310486 4905083 3299234 633830668 26973654 0 3344862 28284141 0 0 0 0 0 0
 259       5 nvme1n1 16478852962 684755 139660303464 4041025697 191545776 3994164 30038357911 3695409309 0 588447592 3442404703 29888 5913 38808742648 936992 0 0";
        let parsed = parse_diskstats(text);
        assert_eq!(parsed, vec![
            ("nvme0n1".to_string(), 3_344_862),
            ("nvme1n1".to_string(), 588_447_592),
        ]);
    }

    /// Highest utilization wins and is named; a device busy 1000ms of a
    /// 2000ms window is 50%.
    #[test]
    fn disk_util_is_the_max_across_devices() {
        let prev = vec![("a".to_string(), 1000_u64), ("b".to_string(), 5000)];
        let cur = vec![("a".to_string(), 1400), ("b".to_string(), 6000)];
        let (name, util) = max_disk_util(&prev, &cur, 2000.0).unwrap();
        assert_eq!(name, "b");
        assert!((util - 0.5).abs() < 1e-9);
    }

    /// Mean and max-core are separate measures: one saturated core among
    /// idle ones must not disappear into the mean.
    #[test]
    fn one_hot_core_shows_in_max_not_mean() {
        let stat_t0 = "\
cpu  1000 0 0 8000 0 0 0 0 0 0
cpu0 1000 0 0 0 0 0 0 0 0 0
cpu1 0 0 0 4000 0 0 0 0 0 0
cpu2 0 0 0 4000 0 0 0 0 0 0";
        let stat_t1 = "\
cpu  2000 0 0 16000 0 0 0 0 0 0
cpu0 2000 0 0 0 0 0 0 0 0 0
cpu1 0 0 0 8000 0 0 0 0 0 0
cpu2 0 0 0 8000 0 0 0 0 0 0";
        let (a0, c0) = parse_proc_stat(stat_t0).unwrap();
        let (a1, c1) = parse_proc_stat(stat_t1).unwrap();
        let mean = cpu_util(a0, a1);
        let (core, max) = max_core_util(&c0, &c1).unwrap();
        assert!((mean - 1000.0 / 9000.0).abs() < 1e-9, "mean {mean}");
        assert_eq!(core, 0);
        assert!((max - 1.0).abs() < 1e-9, "core0 fully busy, got {max}");
    }

    /// iowait is idle-with-an-excuse: an I/O-bound stall must not read as
    /// CPU saturation.
    #[test]
    fn iowait_does_not_count_as_busy() {
        let t0 = parse_proc_stat("cpu 0 0 0 0 0 0 0 0 0 0").unwrap().0;
        let t1 = parse_proc_stat("cpu 0 0 0 500 500 0 0 0 0 0").unwrap().0;
        assert_eq!(cpu_util(t0, t1), 0.0);
    }

    /// The two memory measures diverge exactly by reclaimable cache.
    #[test]
    fn committed_and_cached_measures_are_distinct() {
        let m = parse_meminfo(
            "MemTotal: 1000 kB\nMemFree: 100 kB\nMemAvailable: 600 kB\n",
        )
        .unwrap();
        let (committed, cached) = mem_utils(m);
        assert!((committed - 0.4).abs() < 1e-9, "claimed = 1 - avail/total");
        assert!((cached - 0.9).abs() < 1e-9, "touched = 1 - free/total");
    }

    /// A window with no elapsed time or no shared devices yields nothing
    /// rather than a fabricated zero.
    #[test]
    fn degenerate_windows_yield_none() {
        let d = vec![("a".to_string(), 5_u64)];
        assert!(max_disk_util(&d, &d, 0.0).is_none());
        assert!(max_disk_util(&[], &d, 1000.0).is_none());
    }
}
