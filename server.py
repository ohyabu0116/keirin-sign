#!/usr/bin/env python3
"""
競輪サイン予想サーバー (KEIRIN.JP JSON API ベース)

提供API:
- /api/ping                サーバー死活
- /api/racing_venues       指定日の開催競輪場一覧
- /api/racelist            出走表 (venue, date, raceNo)
- /api/race_result         レース結果 (venue, date, raceNo)
- /api/bulk_fetch          過去レース結果一括取得 (job)
- /api/job/<id>            ジョブ状態
- /api/db/<store>          CRUD (players/raceResults/signConditions/signTracking/discoveries)
- /api/db/migrate          IndexedDB → SQLite 一括投入
- /api/discover            自動サインマイニング
"""
import os, json, ssl, threading, uuid, time, sqlite3, concurrent.futures
from datetime import date, timedelta, datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ── SSL 検証無効化 (macOS対策) ────────────────────────────────
ssl._create_default_https_context = ssl._create_unverified_context

# ── HTTP セッション ───────────────────────────────────────────
try:
    import requests as _req
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    _HEADERS = {
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
        'Accept': 'application/json,text/javascript,*/*;q=0.01',
        'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://keirin.jp/pc/top',
        'X-Requested-With': 'XMLHttpRequest',
    }
    _tls = threading.local()
    def _session():
        if not hasattr(_tls, 's') or _tls.s is None:
            s = _req.Session()
            s.verify = False
            s.headers.update(_HEADERS)
            ad = HTTPAdapter(pool_connections=10, pool_maxsize=20,
                             max_retries=Retry(total=2, backoff_factor=0.5,
                                               status_forcelist=[429,500,502,503,504],
                                               allowed_methods=['GET']))
            s.mount('https://', ad); s.mount('http://', ad)
            _tls.s = s
        return _tls.s
    _USE_REQ = True
except ImportError:
    _USE_REQ = False
    print('[HTTP] requestsライブラリなし → pip install requests を推奨')
    from urllib.request import urlopen, Request

KEIRIN_BASE = 'https://keirin.jp/pc/json'

def http_get_json(params, timeout=12):
    """KEIRIN.JP の JSON エンドポイントを叩く"""
    if _USE_REQ:
        r = _session().get(KEIRIN_BASE, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    else:
        from urllib.parse import urlencode
        url = KEIRIN_BASE + '?' + urlencode(params)
        req = Request(url, headers=_HEADERS)
        with urlopen(req, timeout=timeout) as resp:
            return json.load(resp)

# ── 定数 ──────────────────────────────────────────────────────
PORT = int(os.environ.get('PORT', 8770))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('DATA_DIR', BASE_DIR)
DB_PATH = os.path.join(DATA_DIR, 'keirinsign_data.db')

# 競輪場コード → 名前
VENUE_MAP = {
    '11':'函館','12':'青森','13':'いわき平',
    '21':'弥彦','22':'前橋','23':'取手','24':'宇都宮','25':'大宮','26':'西武園','27':'京王閣','28':'立川',
    '31':'松戸','32':'千葉','34':'川崎','35':'平塚','36':'小田原','37':'伊東','38':'静岡',
    '42':'名古屋','43':'岐阜','44':'大垣','45':'豊橋','46':'富山','47':'松阪','48':'四日市',
    '51':'福井','53':'奈良','54':'向日町','55':'和歌山','56':'岸和田',
    '61':'玉野','62':'広島','63':'防府',
    '71':'高松','73':'小松島','74':'高知','75':'松山',
    '81':'小倉','83':'久留米','84':'武雄','85':'佐世保','86':'別府','87':'熊本',
}
VENUE_NAME_TO_CD = {v: k for k, v in VENUE_MAP.items()}

# フロント store → SQLiteテーブル名
STORE_TABLE = {
    'players':         'players',
    'raceResults':     'race_results',
    'signConditions':  'sign_conditions',
    'signTracking':    'sign_tracking',
    'discoveries':     'discoveries',
    'predictionLogs':  'prediction_logs',
}

# ── DB ────────────────────────────────────────────────────────
_db_lock = threading.Lock()
_race_key_cache = set()   # (date, venue, raceNo)

def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA busy_timeout=5000')
    c.row_factory = sqlite3.Row
    return c

def _init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    c = _conn()
    for t in STORE_TABLE.values():
        c.execute(f'''CREATE TABLE IF NOT EXISTS {t} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            json_data TEXT NOT NULL
        )''')
    c.commit()
    _load_race_keys()
    c.close()
    print(f'[DB] 初期化完了: {DB_PATH}')

def _load_race_keys():
    global _race_key_cache
    c = _conn()
    rows = c.execute('SELECT json_data FROM race_results').fetchall()
    keys = set()
    for r in rows:
        try:
            d = json.loads(r['json_data'])
            k = (str(d.get('date','')), str(d.get('venue','')), str(d.get('raceNo','')))
            if all(k): keys.add(k)
        except: pass
    c.close()
    _race_key_cache = keys
    print(f'[Cache] レースキャッシュ: {len(keys)}件')

def _find_race_id(c, date_s, venue, race_no):
    rows = c.execute('SELECT id, json_data FROM race_results').fetchall()
    for r in rows:
        try:
            d = json.loads(r['json_data'])
            if (str(d.get('date'))==str(date_s) and str(d.get('venue'))==str(venue)
                and str(d.get('raceNo'))==str(race_no)):
                return r['id']
        except: pass
    return None

def db_all(store):
    tbl = STORE_TABLE.get(store)
    if not tbl: return []
    c = _conn()
    rows = c.execute(f'SELECT id, json_data FROM {tbl}').fetchall()
    c.close()
    out = []
    for r in rows:
        try:
            d = json.loads(r['json_data'])
            d['id'] = r['id']
            out.append(d)
        except: pass
    return out

def db_put(store, data):
    tbl = STORE_TABLE.get(store)
    if not tbl: return None
    rec = {k: v for k, v in data.items() if k != 'id'}
    rid = data.get('id')
    with _db_lock:
        c = _conn()
        # raceResults は同一(date,venue,raceNo)を上書き
        if store == 'raceResults' and rec.get('date') and rec.get('venue') and rec.get('raceNo') is not None:
            existing = _find_race_id(c, rec['date'], rec['venue'], rec['raceNo'])
            if existing and not rid:
                rid = existing
        if rid:
            c.execute(f'UPDATE {tbl} SET json_data=? WHERE id=?',
                      (json.dumps(rec, ensure_ascii=False), rid))
            new_id = rid
        else:
            cur = c.execute(f'INSERT INTO {tbl} (json_data) VALUES (?)',
                            (json.dumps(rec, ensure_ascii=False),))
            new_id = cur.lastrowid
        c.commit()
        c.close()
    if store == 'raceResults':
        k = (str(rec.get('date','')), str(rec.get('venue','')), str(rec.get('raceNo','')))
        if all(k): _race_key_cache.add(k)
    return new_id

def db_del(store, rid):
    tbl = STORE_TABLE.get(store)
    if not tbl: return
    with _db_lock:
        c = _conn()
        if store == 'raceResults':
            row = c.execute(f'SELECT json_data FROM {tbl} WHERE id=?',(rid,)).fetchone()
            if row:
                try:
                    d = json.loads(row['json_data'])
                    k = (str(d.get('date','')), str(d.get('venue','')), str(d.get('raceNo','')))
                    _race_key_cache.discard(k)
                except: pass
        c.execute(f'DELETE FROM {tbl} WHERE id=?', (rid,))
        c.commit()
        c.close()

# ── KEIRIN.JP スクレイピング ─────────────────────────────────
def fetch_kaisai(kday):
    """JSJ057: 指定日(YYYYMMDD)の開催情報を取得。
    返値: [{'jyoName','bKeirinCd','gradeIconChar','nitijiIconChar','encPrm', ...}, ...]"""
    j = http_get_json({'type':'JSJ057','kday':kday})
    return j.get('kInfo') or []

def fetch_syusou(encp):
    """JSJ017: 出走表(全レース)を取得"""
    j = http_get_json({'type':'JSJ017','encp':encp})
    return j

def fetch_result(encp):
    """JSJ018: レース結果(全レース)を取得"""
    j = http_get_json({'type':'JSJ018','encp':encp})
    return j

def find_encp(kday, venue_cd):
    """指定日・指定競輪場のencPrmを取得 (キャッシュなし、毎回新規取得)"""
    info = fetch_kaisai(kday)
    for v in info:
        if str(v.get('bKeirinCd')) == str(venue_cd) or str(v.get('KeirinCd')) == str(venue_cd):
            return v.get('encPrm')
    return None

# ── データ正規化 ─────────────────────────────────────────────
def normalize_syusou(jsj017, date_s, venue_name, grade=''):
    """JSJ017の生データ → 各レースの正規化レコード配列"""
    out = []
    for r in jsj017.get('rInfo') or []:
        entries = []
        for s in r.get('sInfo') or []:
            entries.append({
                'syaban': int(s['syaban']),         # 車番
                'regNo': str(s.get('senNo','')).zfill(6),
                'name': (s.get('senName') or '').replace('　',' ').strip(),
                'prefecture': (s.get('huken') or '').replace('　','').strip(),
                'kyaku': s.get('kyaku') or '',     # 逃/両/追
                'note': s.get('assen') or '',      # (補充)/(追加)
            })
        out.append({
            'date': date_s,
            'venue': venue_name,
            'raceNo': int(r['raceNo']),
            'grade': grade,                          # F1/F2/G1/G2/G3/GP
            'syumoku': r.get('syumoku') or '',      # 例: S級特選, A級決勝
            'denTime': r.get('denTime') or '',
            'stTime': r.get('stTime') or '',
            'entries': entries,
            'racerCount': len(entries),              # 7車 or 9車
        })
    return out

def normalize_result(jsj018):
    """JSJ018 → raceNo → {result1, result2, result3, kimari1/2/3, payouts}"""
    out = {}
    for r in jsj018.get('resultList') or []:
        rno_s = (r.get('rclblRaceNo') or '').rstrip('R')
        try:
            rno = int(rno_s)
        except:
            continue
        def first(lst, key, default=None):
            if not lst or not isinstance(lst, list) or not lst[0]: return default
            return lst[0].get(key, default)
        t1 = r.get('tyakui1List') or []
        t2 = r.get('tyakui2List') or []
        t3 = r.get('tyakui3List') or []
        # 3連単払戻
        h3 = r.get('harai3renList') or []
        pay3 = (h3[0].get('kingaku') if h3 else '').replace(',','').replace('円','') if h3 else ''
        try: pay3 = int(pay3) if pay3 else None
        except: pay3 = None
        # 2車単払戻
        h2 = r.get('harai2syaList') or []
        pay2 = (h2[0].get('kingaku') if h2 else '').replace(',','').replace('円','') if h2 else ''
        try: pay2 = int(pay2) if pay2 else None
        except: pay2 = None
        ninki3 = (h3[0].get('ninki') if h3 else '').replace('(','').replace(')','') if h3 else ''
        ninki2 = (h2[0].get('ninki') if h2 else '').replace('(','').replace(')','') if h2 else ''
        try: ninki3 = int(ninki3) if ninki3 else None
        except: ninki3 = None
        try: ninki2 = int(ninki2) if ninki2 else None
        except: ninki2 = None
        out[rno] = {
            'result1': first(t1, 'rclblSyaban'),
            'result2': first(t2, 'rclblSyaban'),
            'result3': first(t3, 'rclblSyaban'),
            'kimari1': first(t1, 'rclblKimari', ''),
            'kimari2': first(t2, 'rclblKimari', ''),
            'kimari3': first(t3, 'rclblKimari', ''),
            'trifectaCombo': h3[0].get('kumi') if h3 else '',
            'exactaCombo': h2[0].get('kumi') if h2 else '',
            'trifectaNinki': ninki3,
            'exactaNinki': ninki2,
            'name1':   first(t1, 'rclblSensyuName', '').replace('　',' '),
            'name2':   first(t2, 'rclblSensyuName', '').replace('　',' '),
            'name3':   first(t3, 'rclblSensyuName', '').replace('　',' '),
            'trifectaPayout': pay3,
            'exactaPayout':   pay2,
        }
    return out

def fetch_day_venue(date_s, venue_name):
    """指定日・指定競輪場の全レース(出走表+結果)を取得"""
    kday = date_s.replace('-','')
    venue_cd = VENUE_NAME_TO_CD.get(venue_name)
    if not venue_cd:
        raise ValueError(f'未知の競輪場: {venue_name}')
    info = fetch_kaisai(kday)
    target = None
    for v in info:
        if (v.get('jyoName') == venue_name or
            str(v.get('bKeirinCd')) == str(venue_cd)):
            target = v
            break
    if not target:
        return []
    encp = target.get('encPrm')
    grade = target.get('gradeIconChar') or ''
    nitiji = target.get('nitijiIconChar') or ''
    if not encp:
        return []
    races = normalize_syusou(fetch_syusou(encp), date_s, venue_name, grade)
    for r in races:
        r['nitiji'] = nitiji
    # 結果も取得（あれば）
    try:
        result_map = normalize_result(fetch_result(encp))
        for r in races:
            res = result_map.get(r['raceNo'])
            if res:
                r.update(res)
    except Exception as e:
        print(f'[WARN] 結果取得失敗 {date_s} {venue_name}: {e}')
    return races

# ── ジョブ管理 ────────────────────────────────────────────────
_jobs = {}

def run_bulk_job(job_id, date_from, date_to, venues=None):
    """過去レース結果の一括取得 (日付範囲、競輪場フィルタ)"""
    job = _jobs[job_id]
    job.update({'status':'running','saved':0,'skipped':0,'errors':0,'done':0,'total':0,'buffer':[]})
    d0 = date.fromisoformat(date_from)
    d1 = date.fromisoformat(date_to)
    all_days = []
    cur = d0
    while cur <= d1:
        all_days.append(cur.isoformat())
        cur += timedelta(days=1)
    job['total_days'] = len(all_days)

    venue_filter = set(venues) if venues else None

    def process_day(ds):
        """1日分の処理: 開催情報取得 → 各場の全レース取得"""
        try:
            kday = ds.replace('-','')
            info = fetch_kaisai(kday)
            n_saved = 0
            for v in info:
                vn = v.get('jyoName')
                if venue_filter and vn not in venue_filter:
                    continue
                encp = v.get('encPrm')
                if not encp:
                    continue
                grade = v.get('gradeIconChar') or ''
                try:
                    races = normalize_syusou(fetch_syusou(encp), ds, vn, grade)
                    result_map = normalize_result(fetch_result(encp))
                    for r in races:
                        res = result_map.get(r['raceNo'])
                        if res: r.update(res)
                        k = (r['date'], r['venue'], str(r['raceNo']))
                        if k in _race_key_cache:
                            # 既存でも結果が空ならアップデート
                            if r.get('result1') is None:
                                job['skipped'] += 1
                                continue
                        job['buffer'].append(r)
                        n_saved += 1
                except Exception as e:
                    job['errors'] += 1
                    print(f'[ERR] {ds} {vn}: {e}')
            return n_saved
        except Exception as e:
            job['errors'] += 1
            print(f'[ERR] {ds}: {e}')
            return 0

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(process_day, ds): ds for ds in all_days}
        for f in concurrent.futures.as_completed(futs):
            if job.get('cancel'):
                break
            try:
                n = f.result()
                job['saved'] += n
            except Exception as e:
                job['errors'] += 1
            job['done'] += 1
            # バッファをDBに書き出し
            if len(job['buffer']) > 50:
                _flush_buffer(job)
    _flush_buffer(job)
    job['status'] = 'done'
    elapsed = time.time() - t0
    print(f'[bulk] {date_from}~{date_to}: 保存{job["saved"]}件 スキップ{job["skipped"]}件 エラー{job["errors"]}件 ({elapsed:.0f}秒)')

def _flush_buffer(job):
    if not job.get('buffer'): return
    buf = job['buffer']; job['buffer'] = []
    # 一括処理用のplayer cache（既存全player分）
    if not job.get('_pcache'):
        job['_pcache'] = {str(p.get('regNo')).zfill(6): True for p in db_all('players') if p.get('regNo')}
    pcache = job['_pcache']
    for r in buf:
        try:
            db_put('raceResults', r)
            # 同時に新選手を登録
            for e in r.get('entries') or []:
                rn = str(e.get('regNo') or '').zfill(6)
                if not rn or rn in pcache: continue
                db_put('players', {
                    'regNo': rn, 'name': e.get('name',''),
                    'prefecture': e.get('prefecture',''), 'kyaku': e.get('kyaku',''), 'grade': '',
                })
                pcache[rn] = True
        except Exception as e:
            job['errors'] += 1
            print(f'[ERR] DB保存失敗: {e}')

def run_today_job(job_id, date_str):
    """指定日の開催全競輪場・全レースを取得"""
    job = _jobs[job_id]
    job.update({'status':'running','saved':0,'skipped':0,'errors':0,'done':0,'total':0,'buffer':[]})
    try:
        kday = date_str.replace('-','')
        info = fetch_kaisai(kday)
        job['total'] = len(info)
        for v in info:
            if job.get('cancel'): break
            vn = v.get('jyoName')
            encp = v.get('encPrm')
            grade = v.get('gradeIconChar') or ''
            if not encp:
                job['done'] += 1
                continue
            try:
                races = normalize_syusou(fetch_syusou(encp), date_str, vn, grade)
                result_map = normalize_result(fetch_result(encp))
                for r in races:
                    res = result_map.get(r['raceNo'])
                    if res: r.update(res)
                    db_put('raceResults', r)
                    auto_register_players_from_race(r)
                    job['saved'] += 1
            except Exception as e:
                job['errors'] += 1
                print(f'[ERR] {date_str} {vn}: {e}')
            job['done'] += 1
        job['status'] = 'done'
        print(f'[today] {date_str}: 保存{job["saved"]}件 エラー{job["errors"]}件')
    except Exception as e:
        job['status'] = 'error'
        job['error_msg'] = str(e)
        print(f'[ERR] today_job: {e}')

# ── 選手DB自動更新 ──────────────────────────────────────────
def auto_register_players_from_race(race_dict):
    """レースデータから新選手を自動登録"""
    if not race_dict.get('entries'): return 0
    existing = {str(p.get('regNo')): p for p in db_all('players') if p.get('regNo')}
    added = 0
    for e in race_dict['entries']:
        rn = str(e.get('regNo') or '').zfill(6)
        if not rn or rn in existing: continue
        db_put('players', {
            'regNo': rn,
            'name': e.get('name',''),
            'prefecture': e.get('prefecture',''),
            'kyaku': e.get('kyaku',''),
            'grade': '',  # 級班はJSJ017にない場合 — syumokuから推測
        })
        added += 1
    return added

# ── サインマイニング ─────────────────────────────────────────
def mine_course_link(min_trials=5, min_rate=90, lookback_range=(2,5)):
    """選手×競輪場×車番×lookback の組み合わせで連動率を集計、高連動パターンを発見"""
    races = db_all('raceResults')
    races = [r for r in races if r.get('result1') and r.get('entries')]
    # (playerRegNo, venue, syaban) → 出走履歴(date順)
    histories = {}
    for r in races:
        for e in r.get('entries') or []:
            rn = str(e.get('regNo') or '').zfill(6)
            sy = e.get('syaban')
            if not rn or not sy: continue
            key = (rn, r.get('venue'), int(sy))
            histories.setdefault(key, []).append(r)
    # 日付順にソート
    for k in histories:
        histories[k].sort(key=lambda r: (r.get('date',''), int(r.get('raceNo',0))))

    out = []
    players = {str(p.get('regNo')): p for p in db_all('players') if p.get('regNo')}
    for (rn, venue, sy), hist in histories.items():
        if len(hist) < min_trials + lookback_range[0]: continue
        for lb in range(lookback_range[0], lookback_range[1]+1):
            trials = 0; hits2 = 0; hits3 = 0
            for i in range(lb, len(hist)):
                curr = hist[i]
                ref = hist[i-lb]
                ref_top3 = [ref.get('result1'), ref.get('result2'), ref.get('result3')]
                ref_top3 = [int(x) for x in ref_top3 if x]
                cur_top3 = [curr.get('result1'), curr.get('result2'), curr.get('result3')]
                cur_top3 = [int(x) for x in cur_top3 if x]
                if len(ref_top3) < 3 or len(cur_top3) < 3: continue
                # 参照レースの上位3着の選手IDが、当該レースで何着以内か
                ref_regs = []
                for bn in ref_top3:
                    for e in ref.get('entries') or []:
                        if int(e.get('syaban',0)) == bn:
                            ref_regs.append(str(e.get('regNo','')).zfill(6)); break
                cur_regs = []
                for bn in cur_top3:
                    for e in curr.get('entries') or []:
                        if int(e.get('syaban',0)) == bn:
                            cur_regs.append(str(e.get('regNo','')).zfill(6)); break
                if len(ref_regs) < 3 or len(cur_regs) < 3: continue
                matches = sum(1 for x in ref_regs if x in cur_regs)
                trials += 1
                if matches >= 2: hits2 += 1
                if matches >= 3: hits3 += 1
            if trials < min_trials: continue
            rate2 = round(hits2/trials*100)
            if rate2 >= min_rate:
                p = players.get(rn)
                out.append({
                    'type': 'COURSE_LINK',
                    'regNo': rn,
                    'playerName': p.get('name','?') if p else '?',
                    'venue': venue,
                    'syaban': sy,
                    'lookback': lb,
                    'trials': trials,
                    'hits2': hits2,
                    'hits3': hits3,
                    'rate2': rate2,
                    'rate3': round(hits3/trials*100) if trials else 0,
                    'desc': f'{p.get("name","?") if p else "?"} {venue}{sy}番 {lb}走前→2艇以上連動 {rate2}% ({hits2}/{trials})',
                })
    out.sort(key=lambda x: (-x['rate2'], -x['trials']))
    return out

def _build_example(r):
    return {
        'date': r.get('date'), 'venue': r.get('venue'), 'raceNo': r.get('raceNo'),
        'result': '-'.join(str(x) for x in [r.get('result1'), r.get('result2'), r.get('result3')] if x),
        'kimari': r.get('kimari1') or '',
    }

def _build_player_map(players, races):
    """選手DBに無いregNoはraceエントリから名前を補完したplayerMapを返す"""
    pm = {str(p.get('regNo')).zfill(6): p for p in players if p.get('regNo')}
    for r in races:
        for e in r.get('entries') or []:
            rn = str(e.get('regNo') or '').zfill(6)
            if rn and rn not in pm and e.get('name'):
                pm[rn] = {'regNo': rn, 'name': e.get('name',''),
                          'prefecture': e.get('prefecture',''),
                          'kyaku': e.get('kyaku','')}
    return pm

def mine_player_syaban_link(min_trials=3, min_rate=70):
    """「ある選手が○番のとき、△番が3着以内に来る確率」を集計
    競輪の代表的なサイン: 特定選手が固定車番に入ると連動車番が決まる"""
    races = db_all('raceResults')
    races = [r for r in races if r.get('result1') and r.get('entries')]
    # (regNo, syaban) → 出走履歴
    hist = {}
    for r in races:
        for e in r.get('entries') or []:
            rn = str(e.get('regNo') or '').zfill(6)
            sy = e.get('syaban')
            if not rn or not sy: continue
            hist.setdefault((rn, int(sy)), []).append(r)
    out = []
    players = _build_player_map(db_all('players'), races)
    for (rn, sy), races_for in hist.items():
        if len(races_for) < min_trials: continue
        # 9車立てMAXまでの各車番ごとに3着以内率を計算
        link_count = {n: 0 for n in range(1, 10)}
        win_count = {n: 0 for n in range(1, 10)}
        for r in races_for:
            top3 = set(int(r[k]) for k in ('result1','result2','result3') if r.get(k))
            for n in range(1, 10):
                if n == sy: continue
                if n in top3:
                    link_count[n] += 1
                if int(r.get('result1') or 0) == n:
                    win_count[n] += 1
        for linked_sy, cnt in link_count.items():
            if cnt == 0: continue
            rate = round(cnt / len(races_for) * 100)
            win_rate = round(win_count[linked_sy] / len(races_for) * 100)
            if rate >= min_rate:
                p = players.get(rn, {})
                # 連動事例 (3着内に来た) と 不発事例
                hits = [r for r in races_for if linked_sy in [int(r.get(k) or 0) for k in ('result1','result2','result3')]]
                misses = [r for r in races_for if linked_sy not in [int(r.get(k) or 0) for k in ('result1','result2','result3')]]
                hits.sort(key=lambda r: r.get('date',''), reverse=True)
                misses.sort(key=lambda r: r.get('date',''), reverse=True)
                out.append({
                    'type': 'PLAYER_SYABAN_LINK',
                    'regNo': rn,
                    'playerName': p.get('name', '?'),
                    'syaban': sy,
                    'linkedSyaban': linked_sy,
                    'trials': len(races_for),
                    'hits': cnt,
                    'rate': rate,
                    'rateWin': win_rate,
                    'examples': [_build_example(r) for r in hits[:10]],
                    'misses': [_build_example(r) for r in misses[:5]],
                    'desc': f'{p.get("name", "?")} が{sy}番のとき → {linked_sy}番が3着以内 {rate}% ({cnt}/{len(races_for)})',
                })
    out.sort(key=lambda x: (-x['rate'], -x['trials']))
    return out

def mine_player_syaban_pair_link(min_trials=3, min_rate=70):
    """「ある選手が○番のとき、△番と□番が両方3着以内」という2車連動を集計
    2車単/2車複の強力なサインになる"""
    races = db_all('raceResults')
    races = [r for r in races if r.get('result1') and r.get('entries')]
    hist = {}
    for r in races:
        for e in r.get('entries') or []:
            rn = str(e.get('regNo') or '').zfill(6)
            sy = e.get('syaban')
            if not rn or not sy: continue
            hist.setdefault((rn, int(sy)), []).append(r)
    out = []
    players = _build_player_map(db_all('players'), races)
    for (rn, sy), races_for in hist.items():
        if len(races_for) < min_trials: continue
        # 各レースで上位3着の車番セットを取得
        top3_sets = []
        for r in races_for:
            t3 = set(int(r[k]) for k in ('result1','result2','result3') if r.get(k))
            top3_sets.append(t3)
        # 全ペア (n1<n2, n1!=sy, n2!=sy) について両方含まれる頻度
        for n1 in range(1, 10):
            if n1 == sy: continue
            for n2 in range(n1+1, 10):
                if n2 == sy: continue
                cnt = sum(1 for t3 in top3_sets if n1 in t3 and n2 in t3)
                if cnt == 0: continue
                rate = round(cnt / len(races_for) * 100)
                if rate >= min_rate:
                    p = players.get(rn, {})
                    hit_races = [r for r in races_for if {n1,n2}.issubset(set(int(r.get(k) or 0) for k in ('result1','result2','result3')))]
                    miss_races = [r for r in races_for if not {n1,n2}.issubset(set(int(r.get(k) or 0) for k in ('result1','result2','result3')))]
                    hit_races.sort(key=lambda r: r.get('date',''), reverse=True)
                    miss_races.sort(key=lambda r: r.get('date',''), reverse=True)
                    out.append({
                        'type': 'PLAYER_SYABAN_PAIR_LINK',
                        'regNo': rn,
                        'playerName': p.get('name', '?'),
                        'syaban': sy,
                        'linkedPair': [n1, n2],
                        'trials': len(races_for),
                        'hits': cnt,
                        'rate': rate,
                        'examples': [_build_example(r) for r in hit_races[:10]],
                        'misses': [_build_example(r) for r in miss_races[:5]],
                        'desc': f'{p.get("name","?")} が{sy}番のとき → {n1}・{n2}両方3着以内 {rate}% ({cnt}/{len(races_for)})',
                    })
    out.sort(key=lambda x: (-x['rate'], -x['trials']))
    return out

def mine_venue_syaban(min_trials=10, min_rate=90):
    """競輪場×車番 別の3着以内率 (90%以上=必ず絡む車番) を集計"""
    races = db_all('raceResults')
    races = [r for r in races if r.get('result1') and r.get('entries')]
    counts = {}  # (venue, syaban) -> {'trials':n, 'in_top3':m}
    for r in races:
        v = r.get('venue')
        top3 = set()
        for k in ('result1','result2','result3'):
            if r.get(k): top3.add(int(r[k]))
        if not top3: continue
        active = set(int(e.get('syaban')) for e in r.get('entries') or [] if e.get('syaban'))
        for sy in active:
            key = (v, sy)
            d = counts.setdefault(key, {'trials':0,'in_top3':0,'wins':0})
            d['trials'] += 1
            if sy in top3:
                d['in_top3'] += 1
            if r.get('result1') and int(r['result1'])==sy:
                d['wins'] += 1
    out = []
    for (v, sy), d in counts.items():
        if d['trials'] < min_trials: continue
        rate3 = round(d['in_top3']/d['trials']*100)
        rate_win = round(d['wins']/d['trials']*100)
        if rate3 >= min_rate or rate_win >= 50:
            out.append({
                'type':'VENUE_SYABAN','venue':v,'syaban':sy,
                'trials':d['trials'],'in_top3':d['in_top3'],'wins':d['wins'],
                'rate3':rate3,'rateWin':rate_win,
                'desc':f'{v} {sy}番 → 3着以内{rate3}% ({d["in_top3"]}/{d["trials"]}) / 1着{rate_win}%',
            })
    out.sort(key=lambda x: -x['rate3'])
    return out

def mine_kyaku_syaban(min_trials=10, min_rate=80):
    """車番×脚質 → 3着以内率"""
    races = db_all('raceResults')
    races = [r for r in races if r.get('result1') and r.get('entries')]
    counts = {}  # (syaban, kyaku) -> {trials, in_top3}
    for r in races:
        top3 = set(int(r[k]) for k in ('result1','result2','result3') if r.get(k))
        for e in r.get('entries') or []:
            sy = int(e.get('syaban') or 0)
            ky = e.get('kyaku') or ''
            if not sy or not ky: continue
            key = (sy, ky)
            d = counts.setdefault(key, {'trials':0,'in_top3':0})
            d['trials'] += 1
            if sy in top3: d['in_top3'] += 1
    out = []
    for (sy, ky), d in counts.items():
        if d['trials'] < min_trials: continue
        rate = round(d['in_top3']/d['trials']*100)
        if rate >= min_rate or rate <= (100-min_rate):
            out.append({
                'type':'KYAKU_SYABAN','syaban':sy,'kyaku':ky,
                'trials':d['trials'],'in_top3':d['in_top3'],'rate':rate,
                'desc':f'{sy}番×脚質「{ky}」 → 3着以内 {rate}% ({d["in_top3"]}/{d["trials"]})',
            })
    out.sort(key=lambda x: -x['rate'])
    return out

def mine_kyaku_pattern(min_trials=5, min_rate=80):
    """脚質構成パターン → 着順傾向のマイニング (簡易版)"""
    races = db_all('raceResults')
    races = [r for r in races if r.get('result1') and r.get('entries')]
    # 脚質構成 → 1着車番分布
    by_pattern = {}
    for r in races:
        ents = r.get('entries') or []
        if not ents: continue
        # 脚質パターン: 逃の人数_両の人数_追の人数_車立て数
        nige = sum(1 for e in ents if e.get('kyaku')=='逃')
        ryo  = sum(1 for e in ents if e.get('kyaku')=='両')
        oi   = sum(1 for e in ents if e.get('kyaku')=='追')
        n = len(ents)
        key = f'逃{nige}/両{ryo}/追{oi}/{n}車'
        win = r.get('result1')
        if not win: continue
        winner_entry = next((e for e in ents if int(e.get('syaban',0))==int(win)), None)
        if not winner_entry: continue
        winner_kyaku = winner_entry.get('kyaku')
        d = by_pattern.setdefault(key, {'trials':0, 'kyaku_count':{}})
        d['trials'] += 1
        d['kyaku_count'][winner_kyaku] = d['kyaku_count'].get(winner_kyaku,0) + 1
    out = []
    for key, d in by_pattern.items():
        if d['trials'] < min_trials: continue
        for kk, cnt in d['kyaku_count'].items():
            rate = round(cnt/d['trials']*100)
            if rate >= min_rate:
                out.append({
                    'type':'KYAKU_PATTERN', 'pattern':key,
                    'winnerKyaku': kk, 'rate': rate, 'trials': d['trials'], 'hits': cnt,
                    'desc': f'構成「{key}」のとき1着は「{kk}」 {rate}% ({cnt}/{d["trials"]})',
                })
    out.sort(key=lambda x: (-x['rate'], -x['trials']))
    return out

# ── HTTPハンドラ ──────────────────────────────────────────────
class Handler(SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _read_body(self):
        ln = int(self.headers.get('Content-Length') or 0)
        if not ln: return {}
        try: return json.loads(self.rfile.read(ln).decode('utf-8'))
        except: return {}

    def do_GET(self):
        u = urlparse(self.path)
        p = u.path
        q = {k: v[0] for k, v in parse_qs(u.query).items()}

        if p == '/api/ping':
            return self._json(200, {'ok': True, 'service': 'keirin-sign', 'time': datetime.now().isoformat()})

        if p == '/api/racing_venues':
            ds = q.get('date')
            if not ds: return self._json(400, {'ok': False, 'error': 'date required (YYYY-MM-DD)'})
            try:
                kday = ds.replace('-','')
                info = fetch_kaisai(kday)
                venues = []
                for v in info:
                    venues.append({
                        'name': v.get('jyoName'),
                        'code': v.get('bKeirinCd') or v.get('KeirinCd'),
                        'grade': v.get('gradeIconChar') or '',
                        'nitiji': v.get('nitijiIconChar') or '',
                        'lastDay': v.get('nitijiIconChar') == '最終日',
                    })
                return self._json(200, {'ok': True, 'date': ds, 'venues': venues, 'count': len(venues)})
            except Exception as e:
                return self._json(500, {'ok': False, 'error': str(e)})

        if p == '/api/racelist':
            venue = q.get('venue'); ds = q.get('date'); rno = q.get('raceNo')
            if not (venue and ds): return self._json(400, {'ok': False, 'error': 'venue & date required'})
            try:
                races = fetch_day_venue(ds, venue)
                if rno:
                    rno = int(rno)
                    races = [r for r in races if r['raceNo'] == rno]
                if not races:
                    return self._json(404, {'ok': False, 'error': '該当レースなし'})
                # 1レース指定なら単体返し
                if rno and len(races) == 1:
                    r = races[0]
                    return self._json(200, {
                        'ok': True,
                        'venue': venue, 'date': ds, 'raceNo': r['raceNo'],
                        'grade': r.get('grade'), 'syumoku': r.get('syumoku'),
                        'racers': [{'syaban': e['syaban'], 'regNo': e['regNo'],
                                    'name': e['name'], 'prefecture': e['prefecture'],
                                    'kyaku': e['kyaku'], 'note': e.get('note','')}
                                   for e in r.get('entries') or []],
                        'denTime': r.get('denTime'), 'stTime': r.get('stTime'),
                    })
                return self._json(200, {'ok': True, 'date': ds, 'venue': venue, 'races': races})
            except Exception as e:
                return self._json(500, {'ok': False, 'error': str(e)})

        if p == '/api/race_result':
            venue = q.get('venue'); ds = q.get('date'); rno = q.get('raceNo')
            try:
                races = fetch_day_venue(ds, venue)
                if rno:
                    rno = int(rno)
                    races = [r for r in races if r['raceNo'] == rno]
                return self._json(200, {'ok': True, 'races': races})
            except Exception as e:
                return self._json(500, {'ok': False, 'error': str(e)})

        if p.startswith('/api/db/'):
            store = p.split('/')[-1]
            if store not in STORE_TABLE: return self._json(404, {'ok': False, 'error': 'store not found'})
            return self._json(200, {'ok': True, 'data': db_all(store)})

        if p == '/api/bulk_status' or p.startswith('/api/job/'):
            jid = q.get('id') or p.split('/')[-1]
            j = _jobs.get(jid)
            if not j: return self._json(404, {'ok': False, 'error': 'job not found'})
            return self._json(200, {'ok': True, **{k:v for k,v in j.items() if k != 'buffer'}})

        if p == '/api/discover':
            min_trials = int(q.get('min_trials', 5))
            min_rate = int(q.get('min_rate', 90))
            try:
                course = mine_course_link(min_trials=min_trials, min_rate=min_rate)
                kyaku  = mine_kyaku_pattern(min_trials=min_trials, min_rate=min_rate)
                venue  = mine_venue_syaban(min_trials=max(min_trials*3,10), min_rate=min_rate)
                kysy   = mine_kyaku_syaban(min_trials=max(min_trials*3,10), min_rate=min_rate)
                psy    = mine_player_syaban_link(min_trials=max(min_trials-2,3), min_rate=min_rate-10)
                psypair= mine_player_syaban_pair_link(min_trials=max(min_trials-2,3), min_rate=min_rate-20)
                return self._json(200, {'ok': True,
                    'courseLink': course, 'kyakuPattern': kyaku,
                    'venueSyaban': venue, 'kyakuSyaban': kysy,
                    'playerSyabanLink': psy,
                    'playerSyabanPairLink': psypair})
            except Exception as e:
                return self._json(500, {'ok': False, 'error': str(e)})

        if p == '/api/stats':
            races = db_all('raceResults')
            players = db_all('players')
            with_result = sum(1 for r in races if r.get('result1'))
            dates = sorted(set(r.get('date','') for r in races if r.get('date')))
            return self._json(200, {'ok': True,
                'total_races': len(races),
                'with_result': with_result,
                'total_players': len(players),
                'date_from': dates[0] if dates else None,
                'date_to': dates[-1] if dates else None,
            })

        # 静的ファイル
        return super().do_GET()

    def do_POST(self):
        u = urlparse(self.path); p = u.path
        body = self._read_body()

        if p.startswith('/api/db/') and not p.endswith('/migrate'):
            store = p.split('/')[-1]
            if store not in STORE_TABLE: return self._json(404, {'ok': False, 'error': 'store not found'})
            try:
                new_id = db_put(store, body)
                return self._json(200, {'ok': True, 'id': new_id})
            except Exception as e:
                return self._json(500, {'ok': False, 'error': str(e)})

        if p == '/api/db/migrate':
            try:
                for store, records in body.items():
                    if store not in STORE_TABLE: continue
                    for rec in records:
                        db_put(store, rec)
                return self._json(200, {'ok': True})
            except Exception as e:
                return self._json(500, {'ok': False, 'error': str(e)})

        if p == '/api/bulk_fetch':
            date_from = body.get('date_from'); date_to = body.get('date_to')
            venues = body.get('venues')
            if not (date_from and date_to): return self._json(400, {'ok': False, 'error': 'date_from & date_to required'})
            jid = uuid.uuid4().hex[:8]
            _jobs[jid] = {'status':'pending','saved':0,'skipped':0,'errors':0,'done':0,'total':0,'buffer':[]}
            threading.Thread(target=run_bulk_job, args=(jid, date_from, date_to, venues), daemon=True).start()
            return self._json(200, {'ok': True, 'jobId': jid})

        if p == '/api/today_fetch':
            ds = body.get('date') or date.today().isoformat()
            jid = uuid.uuid4().hex[:8]
            _jobs[jid] = {'status':'pending','saved':0,'skipped':0,'errors':0,'done':0,'total':0,'buffer':[]}
            threading.Thread(target=run_today_job, args=(jid, ds), daemon=True).start()
            return self._json(200, {'ok': True, 'jobId': jid})

        if p == '/api/job_cancel':
            jid = body.get('id')
            j = _jobs.get(jid)
            if j: j['cancel'] = True
            return self._json(200, {'ok': True})

        if p == '/api/auto_register':
            # 全レースから新選手を自動登録
            races = db_all('raceResults')
            added = 0
            for r in races:
                added += auto_register_players_from_race(r)
            return self._json(200, {'ok': True, 'added': added})

        return self._json(404, {'ok': False, 'error': 'not found'})

    def do_DELETE(self):
        u = urlparse(self.path); p = u.path
        if p.startswith('/api/db/'):
            parts = p.split('/')
            if len(parts) >= 5:
                store = parts[3]; rid = int(parts[4])
                if store not in STORE_TABLE: return self._json(404, {'ok': False, 'error': 'store not found'})
                db_del(store, rid)
                return self._json(200, {'ok': True})
        return self._json(404, {'ok': False, 'error': 'not found'})

def main():
    _init_db()
    os.chdir(BASE_DIR)
    srv = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f'[Server] http://localhost:{PORT}/  (静的+API)')
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\n[Server] 停止')

if __name__ == '__main__':
    main()
