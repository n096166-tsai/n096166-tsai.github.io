#!/usr/bin/env python3
"""
個股期貨資料爬蟲 v7
使用 FinMind 開放資料 API
- TaiwanFuturesDaily：台灣期貨每日成交資料
- 免費使用，無需登入（有 token 可提高上限）
- GitHub Actions 可正常連線
"""

import json, os, sys, time, datetime, requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TOP_N    = 20

# FinMind API 設定
# 若有註冊帳號，把 token 填入 GitHub Secrets 即可提高上限
FINMIND_URL   = 'https://api.finmindtrade.com/api/v4/data'
FINMIND_TOKEN = os.environ.get('FINMIND_TOKEN', '')  # 可選，空白也能用

# 個股期貨的 futures_id 格式：股票代碼（例如 2330、2317）
# 在 FinMind 中，個股期貨的 futures_id 就是股票代碼本身

def ensure_dir():   os.makedirs(DATA_DIR, exist_ok=True)
def tw_now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
def today_str():    return tw_now().strftime('%Y-%m-%d')
def now_str():      return tw_now().strftime('%Y-%m-%d %H:%M')
def load_json(p):   return json.load(open(p, encoding='utf-8')) if os.path.exists(p) else None
def save_json(p,d): json.dump(d, open(p,'w', encoding='utf-8'), ensure_ascii=False, indent=2)
def safe_float(s):
    try:    return float(str(s).replace(',','').replace('+','').strip())
    except: return None

def get_weekdays(s, e):
    tz = datetime.timezone(datetime.timedelta(hours=8))
    start = datetime.datetime.strptime(s,'%Y-%m-%d').replace(tzinfo=tz)
    end   = datetime.datetime.strptime(e,'%Y-%m-%d').replace(tzinfo=tz)
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5: days.append(cur.strftime('%Y-%m-%d'))
        cur += datetime.timedelta(days=1)
    return days

# ── FinMind API 查詢 ──────────────────────────────────────
def fetch_finmind(date_str):
    """
    使用 FinMind TaiwanFuturesDaily 取得指定日期所有期貨成交資料
    個股期貨的 futures_id 為 4 碼股票代碼
    """
    params = {
        'dataset':   'TaiwanFuturesDaily',
        'start_date': date_str,
        'end_date':   date_str,
    }
    if FINMIND_TOKEN:
        params['token'] = FINMIND_TOKEN

    print(f'  [FinMind] 查詢 {date_str}...')
    try:
        resp = requests.get(FINMIND_URL, params=params, timeout=30)
        print(f'  [FinMind] HTTP {resp.status_code}')
        if resp.status_code != 200:
            return []

        data = resp.json()
        if data.get('status') != 200:
            print(f'  [FinMind] API 回傳狀態：{data.get("status")} {data.get("msg","")}')
            return []

        rows = data.get('data', [])
        print(f'  [FinMind] 取得 {len(rows)} 筆原始資料')
        return rows
    except Exception as e:
        print(f'  [FinMind] 錯誤：{e}')
        return []

def filter_stock_futures(rows):
    """
    從所有期貨資料中篩選出個股期貨
    個股期貨的 futures_id 通常是 4 碼數字（股票代碼）
    並且只取近月合約（成交量較大的）
    排除大盤指數期貨（TX、MTX、TE、TF 等）
    """
    # 排除清單（指數期貨、利率期貨等非個股期貨）
    EXCLUDE = {
        'TX','MTX','TE','TF','XIF','GTF','E4F',
        'MXFB','SPF','UDF','UNF','BTF','GDF',
        'G2F','EUR','JPY','GBP','AUD','MXF',
        'NDF','XAF','XBF','XEF',
    }

    # 個股期貨：futures_id 為純數字或含字母但非排除清單
    stock_map = {}  # futures_id -> 最大成交量那筆（合約月份加總）

    for row in rows:
        fid     = str(row.get('futures_id',''))
        vol     = safe_float(row.get('volume', 0)) or 0
        oi      = safe_float(row.get('open_interest'))
        close   = safe_float(row.get('close'))
        session = row.get('trading_session','')

        # 只取正常交易時段（排除盤後）
        if session and session != 'position':
            continue

        # 篩選個股期貨：4碼數字
        if fid in EXCLUDE:
            continue
        if not fid.isdigit() or len(fid) != 4:
            continue
        if vol <= 0:
            continue

        # 同一個股可能有多個到期月，加總成交量
        if fid in stock_map:
            stock_map[fid]['volume'] += vol
            if oi: stock_map[fid]['open_interest'] = (stock_map[fid].get('open_interest') or 0) + oi
            if close: stock_map[fid]['price'] = close  # 取最後一個月的收盤
        else:
            stock_map[fid] = {
                'code': fid, 'name': fid,  # name 先用 code，之後對應名稱
                'volume': vol, 'price': close,
                'open_interest': oi,
            }

    return list(stock_map.values())

# 股票代碼對應中文名稱
STOCK_NAMES = {
    '2330':'台積電','2317':'鴻海','2454':'聯發科','2382':'廣達',
    '2412':'中華電','2308':'台達電','2881':'富邦金','2882':'國泰金',
    '2886':'兆豐金','2891':'中信金','2884':'玉山金','2892':'第一金',
    '2303':'聯電','2344':'華邦電','3711':'日月光投控','2379':'瑞昱',
    '2357':'華碩','2395':'研華','3034':'聯詠','2327':'國巨',
    '2376':'技嘉','2474':'可成','4904':'遠傳','2609':'陽明',
    '2615':'萬海','2603':'長榮','1301':'台塑','1303':'南亞',
    '1326':'台化','2002':'中鋼','5871':'中租控股','2207':'和泰車',
    '6505':'台塑化','2912':'統一超','2801':'彰銀','5880':'合庫金',
    '2883':'開發金','2885':'元大金','2887':'台新金','2888':'新光金',
    '3008':'大立光','3045':'台灣大','2408':'南亞科','2301':'光寶科',
    '2353':'宏碁','2356':'英業達','2377':'微星','2385':'群光',
    '2409':'友達','2449':'京元電子','2458':'義隆電','2492':'華新科',
    '2498':'宏達電','2545':'皇翔','2548':'華固','2601':'益航',
    '2605':'新興','2606':'裕民','2607':'榮運','2610':'華航',
    '2618':'長榮航','2633':'台灣高鐵','2809':'京城銀','2823':'中壽',
    '2832':'台產','2834':'臺企銀','2836':'高雄銀','2838':'聯邦銀',
    '2845':'遠東銀','2849':'安泰銀','2880':'華南金','2889':'國票金',
    '2890':'永豐金','3006':'晶豪科','3017':'奇鋐','3023':'信邦',
    '3026':'禾伸堂','3031':'佰鴻','4938':'和碩','5483':'中美晶',
    '6415':'矽力-KY','6669':'緯穎','8046':'南電','9910':'豐泰',
}

# ── 計算量增排行 ──────────────────────────────────────────
def calc_ranking(today_recs, yest_recs):
    ymap = {r.get('code'): r for r in (yest_recs or [])}
    result = []
    for r in today_recs:
        code  = r.get('code')
        yest  = ymap.get(code)
        vol_t = r.get('volume') or 0
        vol_y = (yest.get('volume') or 0) if yest else 0
        chg   = round((vol_t-vol_y)/vol_y*100, 2) if vol_y > 0 else None
        # 對應中文名稱
        r['name'] = STOCK_NAMES.get(code, code)
        result.append({**r, 'volume_change_pct': chg, 'price_change_pct': None})
    has = sorted([r for r in result if r['volume_change_pct'] is not None],
                 key=lambda x: x['volume_change_pct'], reverse=True)
    no  = sorted([r for r in result if r['volume_change_pct'] is None],
                 key=lambda x: x.get('volume') or 0, reverse=True)
    return (has + no)[:TOP_N]

def update_index(date):
    p   = os.path.join(DATA_DIR, 'index.json')
    idx = load_json(p) or {'dates': []}
    dates = idx.get('dates', [])
    if date not in dates:
        dates.append(date)
        dates.sort(reverse=True)
    idx.update({'dates': dates, 'last_updated': now_str()})
    save_json(p, idx)

def process_date(date_str, prev_str=None):
    print(f'\n── {date_str} ──────────────────────')

    rows = fetch_finmind(date_str)
    if not rows:
        print(f'  → 無資料，跳過（可能為非交易日）')
        return False

    recs = filter_stock_futures(rows)
    print(f'  → 篩選出個股期貨 {len(recs)} 筆')

    if not recs:
        print(f'  → 無個股期貨資料，跳過')
        return False

    save_json(os.path.join(DATA_DIR, f'raw_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'records': recs})

    yest_data = load_json(os.path.join(DATA_DIR, f'raw_{prev_str}.json')) if prev_str else None
    yest_recs = yest_data.get('records',[]) if yest_data else []
    ranking   = calc_ranking(recs, yest_recs)

    save_json(os.path.join(DATA_DIR, f'ranking_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'ranking': ranking})

    print(f'  → 排行前5：')
    for i, r in enumerate(ranking[:5], 1):
        p = f"{r['volume_change_pct']:+.1f}%" if r['volume_change_pct'] is not None else 'N/A'
        print(f'     {i}. {r["name"]}({r["code"]})  成交量:{r.get("volume")}  量增:{p}')
    update_index(date_str)
    return True

def main():
    ensure_dir()
    print(f'\n{"="*50}\n 個股期貨爬蟲 v7  {now_str()}\n{"="*50}')
    if FINMIND_TOKEN:
        print(f'[認證] 使用 Token（提高 API 上限）')
    else:
        print(f'[認證] 未設定 Token，使用免費模式（300次/小時）')

    bs = os.environ.get('BACKFILL_START','').strip()
    be = os.environ.get('BACKFILL_END','').strip()

    if bs and be:
        dates = get_weekdays(bs, be)
        print(f'[補抓] {bs} ~ {be}，共 {len(dates)} 天')
        for i, d in enumerate(dates):
            process_date(d, dates[i-1] if i > 0 else None)
            time.sleep(1)
    else:
        today = today_str()
        tz    = datetime.timezone(datetime.timedelta(hours=8))
        yest  = (datetime.datetime.now(tz)-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(f'[每日] {today}')
        process_date(today, yest)

    print(f'\n✅ 完成！\n')

if __name__ == '__main__':
    main()
