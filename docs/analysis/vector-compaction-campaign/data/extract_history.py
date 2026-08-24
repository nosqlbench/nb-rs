#!/usr/bin/env python3
"""Extract the FULL merge-rate history from every Cassandra compaction archive.

Archive filenames carry the date (compaction.log.YYYY-MM-DD.N.zip), which is more
reliable than mtime. Produces one row per completed vector merge segment across the
whole retained window, so performance can be read against the code timeline rather
than against a single run.
"""
import re, glob, zipfile, os, time, csv, sys, collections

OUT  = os.path.dirname(os.path.abspath(__file__))
PROG = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Compaction I/O progress: (\d+)/(\d+) batches')
SEG  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Flushed segment with (\d+) cells for a total of ([\d.]+)GiB in (\d+) ms')
PRE  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Source pretouch: warmed ([\d,]+) ordinals(?: across (\d+) sources)? in (\d+) ms')

def ts(s): return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))

def all_lines():
    """Every compaction/system log line available, archives plus live files."""
    seen_files = sorted(glob.glob('/var/log/cassandra/compaction.log*') +
                        glob.glob('/var/log/cassandra/system.log*'))
    for f in seen_files:
        if f.endswith('.zip'):
            try:
                with zipfile.ZipFile(f) as zf:
                    for n in zf.namelist():
                        yield from zf.read(n).decode('utf8', 'replace').splitlines()
            except Exception:
                continue
        else:
            try:
                with open(f, errors='replace') as fh:
                    yield from fh
            except Exception:
                continue

def main():
    prog, segs, pres = [], set(), set()
    for l in all_lines():
        m = PROG.search(l)
        if m:
            prog.append((m.group(1), int(m.group(2)), int(m.group(3))))
            continue
        m = SEG.search(l)
        if m:
            segs.add((m.group(1), int(m.group(2)), float(m.group(3)), int(m.group(4))))
            continue
        m = PRE.search(l)
        if m:
            pres.add((m.group(1), int(m.group(2).replace(',', '')), int(m.group(3) or 0), int(m.group(4))))
    prog.sort()

    # segment merges: the counter resets per merge and the total changes
    runs, cur = [], None
    for t, b, tot in prog:
        if cur is None or cur['tot'] != tot or b < cur['lb']:
            if cur: runs.append(cur)
            cur = dict(tot=tot, t0=t, b0=b, t1=t, lb=b)
        else:
            cur['t1'], cur['lb'] = t, b
    if cur: runs.append(cur)

    rows = []
    for s in runs:
        span = ts(s['t1']) - ts(s['t0']); db = s['lb'] - s['b0']
        if span < 30 or db <= 0: continue
        rate = db / (span / 60.0)
        rows.append(dict(date=s['t0'][:10], ts=s['t0'], total_batches=s['tot'],
                         reached=s['lb'], pct=round(100.0 * s['lb'] / s['tot'], 1),
                         minutes=round(span / 60.0, 1), batches_per_min=round(rate),
                         size_class=("large>=25k" if s['tot'] >= 25000 else
                                     "mid 5k-25k" if s['tot'] >= 5000 else "small<5k"),
                         regime=("COLLAPSE" if s['tot'] >= 25000 and rate <= 200 else
                                 "degraded" if s['tot'] >= 25000 and rate < 3000 else "healthy")))
    with open(os.path.join(OUT, 'history-merges.csv'), 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys())); w.writeheader()
        for r in rows: w.writerow(r)
    print(f"  history-merges.csv: {len(rows)} merges, {rows[0]['date']} .. {rows[-1]['date']}")

    srows = []
    for t, cells, gib, ms in sorted(segs):
        if ms <= 0: continue
        srows.append(dict(date=t[:10], ts=t, cells=cells, gib=gib,
                          seconds=round(ms/1000.0, 1), cells_per_s=round(cells/(ms/1000.0)),
                          size_class="standard" if 3e6 <= cells <= 5e6 else "other"))
    if srows:
        with open(os.path.join(OUT, 'history-segments.csv'), 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=list(srows[0].keys())); w.writeheader()
            for r in srows: w.writerow(r)
        print(f"  history-segments.csv: {len(srows)} segments, {srows[0]['date']} .. {srows[-1]['date']}")

    prows = []
    for t, o, src, ms in sorted(pres):
        if ms <= 0: continue
        prows.append(dict(date=t[:10], ts=t, ordinals=o, sources=src, ms=ms,
                          us_per_ordinal=round(ms*1000.0/o, 2)))
    if prows:
        with open(os.path.join(OUT, 'history-pretouch.csv'), 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=list(prows[0].keys())); w.writeheader()
            for r in prows: w.writerow(r)
        print(f"  history-pretouch.csv: {len(prows)} calls, {prows[0]['date']} .. {prows[-1]['date']}")

if __name__ == "__main__":
    main()
