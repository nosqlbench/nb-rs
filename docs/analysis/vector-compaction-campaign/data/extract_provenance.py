#!/usr/bin/env python3
"""Recover, per node restart, exactly what code and flags were running.

Cassandra logs the complete JVM argument vector at startup (CassandraDaemon:634) and
the resolved SAI vector flag set (VectorFeatureFlags:127). Together with jar mtimes
that pins provenance for every run, so results can be attributed to code rather than
to recollection.
"""
import glob, zipfile, re, os, json, csv

ARGS = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?JVM Arguments: \[(.*)\]\s*$')
FLAGS = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Vector feature flags \(all explicit\): (.*)$')
VER  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?Cassandra version: (\S+)')
OUT  = os.path.dirname(os.path.abspath(__file__))

def texts():
    for f in sorted(glob.glob('/var/log/cassandra/system.log*') + glob.glob('/var/log/cassandra/debug.log*')):
        try:
            if f.endswith('.zip'):
                with zipfile.ZipFile(f) as zf:
                    yield "".join(zf.read(n).decode('utf8','replace') for n in zf.namelist())
            else:
                yield open(f, errors='replace').read()
        except Exception:
            continue

boots = {}
for txt in texts():
    for l in txt.splitlines():
        for pat, key in ((ARGS,'jvm_args'), (FLAGS,'vector_flags'), (VER,'cassandra_version')):
            m = pat.search(l)
            if m:
                boots.setdefault(m.group(1), {})[key] = m.group(2).strip()
                break

rows = []
for boot in sorted(boots):
    b = boots[boot]
    args = b.get('jvm_args','')
    jv = sorted(set(re.findall(r'-Djvector\.[\w.]+=\S+?(?=,|\]|$)', args)))
    sai = sorted(set(re.findall(r'-Dcassandra\.sai\.[\w.]+=\S+?(?=,|\]|$)', args)))
    rows.append(dict(
        boot=boot,
        cassandra_version=b.get('cassandra_version',''),
        jvector_flags=" ".join(x.replace('-Djvector.','') for x in jv),
        sai_flag_count=len(sai),
        vector_feature_flags=b.get('vector_flags','')[:400],
    ))
with open(os.path.join(OUT,'provenance-boots.csv'),'w',newline='') as fh:
    w=csv.DictWriter(fh, fieldnames=list(rows[0].keys())); w.writeheader()
    for r in rows: w.writerow(r)

for r in rows:
    print(f"\n  BOOT {r['boot']}  (cassandra {r['cassandra_version']})")
    print(f"    jvector flags : {r['jvector_flags'] or '(none)'}")
    print(f"    sai flags     : {r['sai_flag_count']}")
print(f"\n  -> provenance-boots.csv ({len(rows)} boots)")
