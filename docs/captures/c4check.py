#!/usr/bin/env python3
"""Cycle-4 check. PRIMARY metric is segment-build throughput in cells/s.

Why not the batch counter: batch counts are not comparable across merges --
ordinals-per-batch varies (the cycle-3 collapse merge carried 4,096/batch while
the four healthy ~31k merges plainly carried far fewer). 'Flushed segment with
N cells for a total of X GiB in M ms' has a fixed unit and a real denominator.
"""
import re, glob, zipfile, time, os, sys

CUT = sys.argv[1] if len(sys.argv) > 1 else "2026-08-24 06:54:00"
CUTE = time.mktime(time.strptime(CUT, "%Y-%m-%d %H:%M:%S"))
SEG  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Flushed segment with (\d+) cells for a total of ([\d.]+)GiB in (\d+) ms')
PROG = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Compaction I/O progress: (\d+)/(\d+) batches')
PRE  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Source pretouch: warmed ([\d,]+) ordinals(?: across (\d+) sources)? in (\d+) ms')

lines = []
for z in [z for z in glob.glob('/var/log/cassandra/*.zip') if os.path.getmtime(z) >= CUTE]:
    try:
        with zipfile.ZipFile(z) as zf:
            for n in zf.namelist(): lines += zf.read(n).decode('utf8', 'replace').splitlines()
    except Exception: pass
for f in glob.glob('/var/log/cassandra/*.log'):
    try: lines += open(f, errors='replace').read().splitlines()
    except Exception: pass

def med(x):
    x = sorted(x); n = len(x)
    if not n: return 0
    return x[n//2] if n % 2 else (x[n//2-1] + x[n//2]) / 2.0

segs = set()
for l in lines:
    m = SEG.search(l)
    if m and m.group(1) >= CUT:
        segs.add((m.group(1), int(m.group(2)), float(m.group(3)), int(m.group(4))))
segs = sorted(segs)

print("=== PRIMARY: segment builds (cells/s) ===")
print("  cycle-3 baseline: standard 3.966M-cell segment median 277 s = 14,318 cells/s, 67 MiB/s")
print("                    collapse trough 762-828 s = 4,792-6,229 cells/s")
std = [(t, c, g, ms) for t, c, g, ms in segs if 3_000_000 <= c <= 5_000_000]
big = [(t, c, g, ms) for t, c, g, ms in segs if c > 5_000_000]
if std:
    rates = [c/(ms/1000.0) for _, c, _, ms in std]
    print(f"  standard segments n={len(std)}  MEDIAN {med(rates):,.0f} cells/s"
          f"  (median {med([ms/1000.0 for *_ , ms in std]):.0f} s)")
    for t, c, g, ms in std[-3:]:
        print(f"    {t[11:]}  {c:>11,} cells  {g:5.1f} GiB  {ms/1000.0:6.0f} s -> {c/(ms/1000.0):>8,.0f} cells/s")
else:
    print("  standard segments: none yet")
for t, c, g, ms in big:
    print(f"  LARGE  {t[11:]}  {c:>11,} cells  {g:6.1f} GiB  {ms/1000.0:7.0f} s -> {c/(ms/1000.0):>8,.0f} cells/s")

prog = []
for l in lines:
    m = PROG.search(l)
    if m and m.group(1) >= CUT: prog.append((m.group(1), int(m.group(2)), int(m.group(3))))
prog.sort()
def ts(s): return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))
segl = []; cur = None
for t, b, tot in prog:
    if cur is None or cur['tot'] != tot or b < cur['lb']:
        if cur: segl.append(cur)
        cur = {'tot': tot, 't0': t, 'b0': b, 't1': t, 'lb': b}
    else: cur['t1'], cur['lb'] = t, b
if cur: segl.append(cur)
print(f"\n=== SECONDARY: merges (batch counter -- compare only within a batch size) ===")
print(f"  merges={len(segl)}  last={prog[-1][0] if prog else 'n/a'}")
for s in segl:
    span = ts(s['t1']) - ts(s['t0']); db = s['lb'] - s['b0']
    if s['tot'] < 25000 or span < 30 or db <= 0: continue
    r = db/(span/60.0)
    done = "DONE" if s['lb'] >= s['tot']*0.98 else f"PARTIAL {100.0*s['lb']/s['tot']:.0f}%"
    print(f"  {s['tot']:,} @{s['t0'][5:16]}  {r:8,.0f} b/min [{done}]"
          f"{'  *** 39-46 BAND ***' if 39 <= r <= 46 else ''}")

pre = set()
for l in lines:
    m = PRE.search(l)
    if m and m.group(1) >= CUT:
        pre.add((m.group(1), int(m.group(2).replace(',', '')), int(m.group(3) or 0), int(m.group(4))))
pre = sorted(pre)
if pre:
    zero = sum(1 for p in pre if p[3] == 0)
    bigp = [p for p in pre if p[1] > 10_000_000]
    print(f"\n=== pretouch: {len(pre)} calls, {zero} at 0 ms, cumulative {sum(p[3] for p in pre)/1000.0:.1f} s ===")
    print(f"  >10M-ordinal calls: n={len(bigp)} cum={sum(p[3] for p in bigp)/1000.0:.1f} s"
          f"  (cycle 3: 2 calls = 484.0 s = 67% of total)")
    for t, o, src, ms in bigp:
        print(f"    {t[11:]}  {o:,} ord / {src} src  {ms:,} ms = {ms/60000.0:.1f} min")
    for t, o, src, ms in pre[-2:]:
        print(f"    recent {t[11:]}  {o:,} ord / {src} src  {ms:,} ms")
