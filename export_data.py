#!/usr/bin/env python3
"""SQLite DB → boatsign_export.json 相当 (keirinsign_export.json) を生成。
GitHub Pages の外出モードで IndexedDB にシードするためのファイル。"""
import json, sqlite3, os, sys
BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, 'keirinsign_data.db')
OUT = os.path.join(BASE, 'keirinsign_export.json')

if not os.path.exists(DB):
    print(f'DB not found: {DB}')
    sys.exit(1)

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
data = {}
for store, tbl in [('players','players'),('raceResults','race_results'),
                   ('signConditions','sign_conditions'),('signTracking','sign_tracking'),
                   ('discoveries','discoveries')]:
    rows = conn.execute(f'SELECT id, json_data FROM {tbl}').fetchall()
    out = []
    for r in rows:
        try:
            d = json.loads(r['json_data'])
            d['id'] = r['id']
            out.append(d)
        except: pass
    data[store] = out
    print(f'  {store}: {len(out)}件')

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)
print(f'\nExported → {OUT} ({os.path.getsize(OUT)/1024/1024:.1f}MB)')
