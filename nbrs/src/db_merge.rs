// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Multi-db merge for `nbrs summary` and `nbrs plot`.
//!
//! Given N metrics dbs from separate sessions of (typically)
//! the same workload, produce a single temp db whose rows
//! present them as one logical session: same `metric_instance`
//! per (metric, labels-without-session), all `sample_value`
//! rows accumulated under it. Summary and plot then run their
//! standard pipelines against the merged db, getting averages
//! / counts / aggregates across every input.
//!
//! ## Why not ATTACH and views?
//!
//! SQLite ATTACH + UNION ALL views works for read-only
//! cross-db reads, but `SqliteReporter`'s queries assume
//! `metric_instance.id` is a stable identity that joins to
//! `sample_value.instance_id`. Bridging IDs across attached
//! dbs would require rewriting every query. A temp merged db
//! keeps the existing reporter code unchanged.
//!
//! ## Session-label stripping
//!
//! Each `metric_instance.spec` carries a `session="…"` label
//! that's distinct per session by construction. Without
//! stripping it, two dbs from the same workload produce two
//! distinct `metric_instance` rows, and the summary would
//! show duplicate rows. Stripping `session=` before the
//! merge lets identical (metric, labels) collapse to one
//! row whose sample_values include every input's data.
//!
//! ## API
//!
//! [`merge_dbs`] takes a non-empty list of input db paths
//! and returns a temp file path holding the merged db. The
//! caller owns the temp file's lifetime — typically dropped
//! at process exit.

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection};

/// Merge `inputs` into a single temp db. Returns the path of
/// the temp db. The temp file persists for the process lifetime
/// (the caller can `std::fs::remove_file` to clean up early).
///
/// Algorithm:
/// 1. Copy `inputs[0]` to a temp file (this preserves schema +
///    its rows verbatim so the schema is inherited).
/// 2. Strip `session="…"` from every `metric_instance.spec` in
///    the merged db so subsequent inserts with stripped specs
///    collide on UNIQUE(spec) and merge.
/// 3. For each remaining input, ATTACH and:
///    a. INSERT OR IGNORE every metric_instance with stripped
///       spec — duplicates are silently skipped (their data
///       lands in the existing row via remapped sample_values).
///    b. Build a remap map from src's metric_instance_id to
///       merged metric_instance_id (matched by stripped spec).
///    c. INSERT every sample_value row using the remap.
///    d. Same dedup-and-insert for label_key, label_value,
///       label_set, label_set_entry. Schema dedup paths use
///       INSERT OR IGNORE; the merge's queries don't depend
///       on label_set IDs being stable, only on
///       metric_instance.spec.
///    e. Carry forward session_metadata: stored summary and
///       plot specs are preserved (last-input wins on key
///       collision).
pub fn merge_dbs(inputs: &[PathBuf]) -> Result<PathBuf, String> {
    if inputs.is_empty() {
        return Err("merge_dbs: at least one input db is required".to_string());
    }
    let temp_path = std::env::temp_dir().join(format!(
        "nbrs_merged_{}_{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    ));
    // Step 1: byte-copy the first input.
    std::fs::copy(&inputs[0], &temp_path)
        .map_err(|e| format!("copy '{}' → '{}': {e}",
            inputs[0].display(), temp_path.display()))?;

    let conn = Connection::open(&temp_path)
        .map_err(|e| format!("open merged db: {e}"))?;

    // Step 2: strip session labels from the seed db's
    // metric_instance.spec. Done in-place so subsequent
    // inserts with stripped specs collide.
    strip_session_labels_in_place(&conn)
        .map_err(|e| format!("strip session labels: {e}"))?;

    // Steps 3a–e: merge each remaining input.
    for src_path in &inputs[1..] {
        merge_one(&conn, src_path)
            .map_err(|e| format!("merge '{}': {e}", src_path.display()))?;
    }

    Ok(temp_path)
}

/// Update `metric_instance.spec` rows in place, stripping
/// `session="…",` (or `,session="…"` at end-of-list) so that
/// specs from different sessions of the same workload become
/// equal.
fn strip_session_labels_in_place(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("SELECT id, spec FROM metric_instance")?;
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);
    let mut update = conn.prepare("UPDATE metric_instance SET spec = ?1 WHERE id = ?2")?;
    for (id, spec) in rows {
        let stripped = strip_session_label(&spec);
        if stripped != spec {
            update.execute(params![stripped, id])?;
        }
    }
    Ok(())
}

/// Strip the `session="…"` label from a metric_instance spec.
/// Handles three positions: leading (`{session="x",rest}`),
/// middle (`a,session="x",rest`), trailing (`a,session="x"}`).
/// Quoted value boundaries are honored so a value containing
/// commas wouldn't fool the splitter — but in practice session
/// values are session-id strings without internal commas.
pub fn strip_session_label(spec: &str) -> String {
    let Some(open) = spec.find('{') else { return spec.to_string(); };
    let Some(close) = spec.rfind('}') else { return spec.to_string(); };
    if close <= open + 1 { return spec.to_string(); }
    let body = &spec[open + 1..close];
    let parts: Vec<&str> = body.split(',').collect();
    let kept: Vec<&str> = parts.iter().copied()
        .filter(|p| {
            let p = p.trim_start();
            !p.starts_with("session=")
        })
        .collect();
    if kept.len() == parts.len() {
        return spec.to_string();
    }
    let new_body = kept.join(",");
    format!("{}{{{}}}", &spec[..open], new_body)
}

fn merge_one(merged: &Connection, src_path: &Path) -> rusqlite::Result<()> {
    merged.execute("ATTACH DATABASE ? AS src",
        params![src_path.to_string_lossy().as_ref()])?;

    // Insert metric_family rows that don't already exist
    // (UNIQUE(name, type) handles dedup).
    merged.execute(
        "INSERT OR IGNORE INTO main.metric_family (name, type, unit, help) \
         SELECT name, type, unit, help FROM src.metric_family",
        [],
    )?;

    // label_key / label_value / label_set / label_set_entry:
    // not strictly needed for summary/plot's queries (which
    // operate off metric_instance.spec), but copy for
    // completeness so the merged db is self-consistent.
    merged.execute(
        "INSERT OR IGNORE INTO main.label_key (key) \
         SELECT key FROM src.label_key",
        [],
    )?;
    merged.execute(
        "INSERT OR IGNORE INTO main.label_value (value) \
         SELECT value FROM src.label_value",
        [],
    )?;
    merged.execute(
        "INSERT OR IGNORE INTO main.label_set (hash) \
         SELECT hash FROM src.label_set",
        [],
    )?;
    // label_set_entry has no UNIQUE — best-effort copy with
    // remapping by hash → new set_id and key/value lookup.
    // We skip rewiring it precisely because summary/plot
    // never read it directly. The reporter only needs
    // metric_instance.spec and sample_value.
    let _ = merged.execute(
        "INSERT INTO main.label_set_entry (set_id, key_id, value_id) \
         SELECT \
            (SELECT id FROM main.label_set WHERE hash = (SELECT hash FROM src.label_set WHERE id = src.label_set_entry.set_id)), \
            (SELECT id FROM main.label_key WHERE key = (SELECT key FROM src.label_key WHERE id = src.label_set_entry.key_id)), \
            (SELECT id FROM main.label_value WHERE value = (SELECT value FROM src.label_value WHERE id = src.label_set_entry.value_id)) \
         FROM src.label_set_entry",
        [],
    );

    // metric_instance: INSERT OR IGNORE with the spec
    // pre-stripped of session=. Specs that already exist (from
    // the seed db or earlier merges) collide on the UNIQUE
    // constraint and are silently skipped — but their family
    // and label_set lookups happen via name/hash so the new
    // db's row points to local IDs.
    //
    // We do this row-by-row because the spec rewrite isn't
    // expressible as plain SQL (no regex on this build's
    // sqlite). Cost is modest — instance counts are small
    // (one row per distinct label set per metric).
    let mut select = merged.prepare(
        "SELECT mi.id, mi.spec, mf.name, mf.type, ls.hash \
         FROM src.metric_instance mi \
         JOIN src.metric_family mf ON mi.family_id = mf.id \
         JOIN src.label_set ls ON mi.label_set_id = ls.id"
    )?;
    let src_rows: Vec<(i64, String, String, String, i64)> = select
        .query_map([], |r| Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, i64>(4)?,
        )))?
        .filter_map(|r| r.ok())
        .collect();
    drop(select);

    let mut insert = merged.prepare(
        "INSERT OR IGNORE INTO main.metric_instance (family_id, label_set_id, spec) \
         VALUES (\
           (SELECT id FROM main.metric_family WHERE name = ?1 AND type = ?2), \
           (SELECT id FROM main.label_set WHERE hash = ?3), \
           ?4)"
    )?;
    let mut find_merged_id = merged.prepare(
        "SELECT id FROM main.metric_instance WHERE spec = ?1"
    )?;

    let mut remap: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for (src_id, spec, fam_name, fam_type, hash) in src_rows {
        let stripped = strip_session_label(&spec);
        insert.execute(params![fam_name, fam_type, hash, stripped])?;
        let merged_id: i64 = find_merged_id.query_row(params![stripped], |r| r.get(0))?;
        remap.insert(src_id, merged_id);
    }
    drop(insert);
    drop(find_merged_id);

    // sample_value: insert every row with remapped instance_id.
    let mut select_sv = merged.prepare(
        "SELECT instance_id, timestamp_ms, interval_ms, count, sum, min, max, mean, \
                stddev, p50, p75, p90, p95, p98, p99, p999 \
         FROM src.sample_value"
    )?;
    let mut insert_sv = merged.prepare(
        "INSERT INTO main.sample_value \
         (instance_id, timestamp_ms, interval_ms, count, sum, min, max, mean, \
          stddev, p50, p75, p90, p95, p98, p99, p999) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)"
    )?;
    let mut sv_iter = select_sv.query([])?;
    while let Some(r) = sv_iter.next()? {
        let src_id: i64 = r.get(0)?;
        let Some(&new_id) = remap.get(&src_id) else { continue; };
        insert_sv.execute(params![
            new_id,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, Option<i64>>(3)?,
            r.get::<_, Option<f64>>(4)?,
            r.get::<_, Option<f64>>(5)?,
            r.get::<_, Option<f64>>(6)?,
            r.get::<_, Option<f64>>(7)?,
            r.get::<_, Option<f64>>(8)?,
            r.get::<_, Option<f64>>(9)?,
            r.get::<_, Option<f64>>(10)?,
            r.get::<_, Option<f64>>(11)?,
            r.get::<_, Option<f64>>(12)?,
            r.get::<_, Option<f64>>(13)?,
            r.get::<_, Option<f64>>(14)?,
            r.get::<_, Option<f64>>(15)?,
        ])?;
    }
    drop(sv_iter);
    drop(select_sv);
    drop(insert_sv);

    // session_metadata: preserve every key. Last-input wins on
    // collisions so the user can layer override specs by db
    // ordering.
    merged.execute(
        "INSERT OR REPLACE INTO main.session_metadata (key, value) \
         SELECT key, value FROM src.session_metadata",
        [],
    )?;

    merged.execute("DETACH DATABASE src", [])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_session_label_in_middle() {
        let spec = "recall@10.mean{session=\"abc\",profile=\"label_03\",k=\"10\"}";
        let stripped = strip_session_label(spec);
        assert_eq!(stripped, "recall@10.mean{profile=\"label_03\",k=\"10\"}");
    }

    #[test]
    fn strips_session_label_at_start() {
        let spec = "metric{session=\"x\",a=\"1\"}";
        assert_eq!(strip_session_label(spec), "metric{a=\"1\"}");
    }

    #[test]
    fn no_change_when_no_session_label() {
        let spec = "metric{profile=\"a\",k=\"10\"}";
        assert_eq!(strip_session_label(spec), spec);
    }

    #[test]
    fn no_change_when_no_braces() {
        assert_eq!(strip_session_label("metric"), "metric");
    }
}
