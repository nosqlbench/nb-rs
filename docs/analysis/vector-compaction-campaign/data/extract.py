#!/usr/bin/env python3
"""Regenerate every CSV in this directory from the Cassandra logs.

Cycle 3 comes from the frozen distillate committed at docs/captures/cycle3-logdata/;
cycle 4 from the live logs plus any .zip archives written after its cutoff. Run from
the nb-rs repo root. Safe to re-run: it only reads logs and rewrites the CSVs.
"""
import re, gzip, glob, zipfile, os, time, csv, sys

OUT = os.path.dirname(os.path.abspath(__file__))
C3_CUT, C3_ZERO = "2026-08-23 19:35:00", "2026-08-23 19:35:00"
C4_CUT, C4_ZERO = "2026-08-24 06:54:00", "2026-08-24 06:54:50"
WINDOW = 1_048_576          # jvector.compaction.sourcePretouchWindowNodes

SEG  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Flushed segment with (\d+) cells for a total of ([\d.]+)GiB in (\d+) ms')
PRE  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Source pretouch: warmed ([\d,]+) ordinals(?: across (\d+) sources)? in (\d+) ms')
PROG = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Compaction I/O progress: (\d+)/(\d+) batches')

def ts(s): return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))

def cycle3_lines():
    p = os.path.join(OUT, '..', '..', '..', 'captures', 'cycle3-logdata', 'cycle3-evidence.log.gz')
    return gzip.open(os.path.normpath(p), 'rt').read().splitlines()

def cycle4_lines():
    cut = ts(C4_CUT); out = []
    # archives first -- system.log rotates mid-run and a *.log-only glob loses most of it
    for z in [z for z in glob.glob('/var/log/cassandra/*.zip') if os.path.getmtime(z) >= cut]:
        try:
            with zipfile.ZipFile(z) as zf:
                for n in zf.namelist():
                    out += zf.read(n).decode('utf8', 'replace').splitlines()
        except Exception:
            pass
    for f in glob.glob('/var/log/cassandra/*.log'):
        try: out += open(f, errors='replace').read().splitlines()
        except Exception: pass
    return out

def segments(lines, cut, zero):
    seen = set()
    for l in lines:
        m = SEG.search(l)
        if m and m.group(1) >= cut:
            seen.add((m.group(1), int(m.group(2)), float(m.group(3)), int(m.group(4))))
    rows = []
    for t, cells, gib, ms in sorted(seen):
        if ms <= 0: continue
        rows.append(dict(ts=t, t_min=round((ts(t)-ts(zero))/60.0, 1), cells=cells,
                         gib=gib, seconds=round(ms/1000.0, 1),
                         cells_per_s=round(cells/(ms/1000.0)),
                         mib_per_s=round(gib*1024/(ms/1000.0), 1),
                         size_class="standard" if 3e6 <= cells <= 5e6 else "large"))
    return rows

def pretouch(lines, cut, zero):
    seen = set()
    for l in lines:
        m = PRE.search(l)
        if m and m.group(1) >= cut:
            seen.add((m.group(1), int(m.group(2).replace(',', '')), int(m.group(3) or 0), int(m.group(4))))
    rows = []
    for t, ords, src, ms in sorted(seen):
        if ms <= 0 or src == 0: continue
        ops = ords/src
        rows.append(dict(ts=t, t_min=round((ts(t)-ts(zero))/60.0, 1), ordinals=ords, sources=src,
                         ordinals_per_source=round(ops), windows_per_source=round(ops/WINDOW, 2),
                         ms=ms, us_per_ordinal=round(ms*1000.0/ords, 2)))
    return rows

def merges(lines, cut, zero, min_total=25000):
    pts = []
    for l in lines:
        m = PROG.search(l)
        if m and m.group(1) >= cut:
            pts.append((m.group(1), int(m.group(2)), int(m.group(3))))
    pts.sort()
    segs, cur = [], None
    for t, b, tot in pts:
        if cur is None or cur['tot'] != tot or b < cur['lb']:
            if cur: segs.append(cur)
            cur = dict(tot=tot, t0=t, b0=b, t1=t, lb=b)
        else:
            cur['t1'], cur['lb'] = t, b
    if cur: segs.append(cur)
    rows = []
    for s in segs:
        span = ts(s['t1']) - ts(s['t0']); db = s['lb'] - s['b0']
        if s['tot'] < min_total or span < 30 or db <= 0: continue
        rows.append(dict(ts=s['t0'], t_min=round((ts(s['t0'])-ts(zero))/60.0, 1),
                         total_batches=s['tot'], reached=s['lb'],
                         pct=round(100.0*s['lb']/s['tot'], 1),
                         minutes=round(span/60.0, 1),
                         batches_per_min=round(db/(span/60.0)),
                         complete=s['lb'] >= s['tot']*0.98))
    return rows

def write(name, rows):
    if not rows:
        print(f"  {name}: no rows"); return
    p = os.path.join(OUT, name)
    with open(p, 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows: w.writerow(r)
    print(f"  {name}: {len(rows)} rows")

if __name__ == "__main__":
    c3, c4 = cycle3_lines(), cycle4_lines()
    for cyc, lines, cut, zero in (("cycle3", c3, C3_CUT, C3_ZERO), ("cycle4", c4, C4_CUT, C4_ZERO)):
        write(f"{cyc}-segments.csv", segments(lines, cut, zero))
        write(f"{cyc}-pretouch.csv", pretouch(lines, cut, zero))
        write(f"{cyc}-large-merges.csv", merges(lines, cut, zero))
