#!/usr/bin/env python3
"""
個股期貨資料爬蟲 v5
使用 Yahoo Finance API 抓取台灣個股期貨資料
台灣個股期貨代碼格式：{股票代碼}F.TW（例如 2330F.TW）
"""

import json, os, sys, time, datetime, requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TOP_N    = 20

# 台灣主要個股期貨清單（股票代碼 + F.TW）
# 這裡列出成交量較大的個股期貨
STOCK_FUTURES = [
    ('2330', '台積電'),('2317', '鴻海'),('2454', '聯發科'),
    ('2382', '廣達'),('2412', '中華電'),('2308', '台達電'),
    ('2881', '富邦金'),('2882', '國泰金'),('2886', '兆豐金'),
    ('2891', '中信金'),('2884', '玉山金'),('2892', '第一金'),
    ('2303', '聯電'),('2344', '華邦電'),('3711', '日月光投控'),
    ('2379', '瑞昱'),('2408', '南亞科'),('2357', '華碩'),
    ('2395', '研華'),('3034', '聯詠'),('2327', '國巨'),
    ('2376', '技嘉'),('2474', '可成'),('4904', '遠傳'),
    ('2609', '陽明'),('2615', '萬海'),('2603', '長榮'),
    ('1301', '台塑'),('1303', '南亞'),('1326', '台化'),
    ('2002', '中鋼'),('5871', '中租控股'),('2207', '和泰車'),
    ('2408', '南亞科'),('6505', '台塑化'),('2912', '統一超'),
    ('2801', '彰銀'),('5880', '合庫金'),('2883', '開發金'),
    ('2885', '元大金'),('2887', '台新金'),('2888', '新光金'),
    ('3008', '大立光'),('2367', '燿華'),('2401', '凌陽'),
    ('3045', '台灣大'),('2468', '華經'),('2Tiger', '虎航'),
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
}

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

def date_to_unix(date_str):
    tz = datetime.timezone(datetime.timedelta(hours=8))
    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=tz)
    return int(dt.timestamp())

# ── Yahoo Finance API ──────────────────────────────────
def fetch_yahoo_quote(symbol):
    """取得單一個股期貨的即時報價"""
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
    params = {'interval': '1d', 'range': '5d'}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        result = data.get('chart', {}).get('result', [])
        if not result:
            return None
        r = result[0]
        meta = r.get('meta', {})
        vol  = meta.get('regularMarketVolume') or meta.get('volume')
        price = meta.get('regularMarketPrice') or meta.get('price')
        if vol is None:
            # 從 indicators 取最後一筆成交量
            indicators = r.get('indicators', {})
            quotes = indicators.get('quote', [{}])[0]
            vols = quotes.get('volume', [])
            vol = vols[-1] if vols else None
            closes = quotes.get('close', [])
            price = closes[-1] if closes else price
        return {'volume': vol, 'price': price}
    except Exception as e:
        return None

def fetch_all_futures(date_str):
    """
    抓取所有個股期貨的當日資料
    Yahoo Finance 個股期貨格式：{代碼}F.TW
    """
    print(f'  [Yahoo] 開始抓取 {date_str} 資料...')
    records = []
    
    for code, name in STOCK_FUTURES:
        # 台灣個股期貨在 Yahoo Finance 的代號
        symbol = f'{code}F.TW'
        result = fetch_yahoo_quote(symbol)
        
        if result and result.get('volume') and result['volume'] > 0:
            records.append({
                'code':          code,
                'name':          name,
                'volume':        float(result['volume']),
                'price':         result.get('price'),
                'open_interest': None,
            })
        time.sleep(0.3)  # 避免請求太快
    
    print(f'  [Yahoo] 取得 {len(records)} 筆有效資料')
    return records

# ── 備用：直接用 Yahoo Finance 查詢熱門台股期貨 ──────────
def fetch_via_screener():
    """
    使用 Yahoo Finance screener 取得台灣期貨資料
    """
    url = 'https://query1.finance.yahoo.com/v1/finance/screener'
    payload = {
        "offset": 0, "size": 100,
        "sortField": "dayvolume", "sortType": "DESC",
        "quoteType": "FUTURE",
        "query": {
            "operator": "AND",
            "operands": [
                {"operator": "EQ", "operands": ["exchange", "TAI"]},
            ]
        },
        "userId": "", "userIdType": "guid"
    }
    try:
        resp = requests.post(url, json=payload, headers={
            **HEADERS, 'Content-Type': 'application/json'
        }, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            quotes = data.get('finance',{}).get('result',[{}])[0].get('quotes',[])
            if quotes:
                print(f'  [Screener] 取得 {len(quotes)} 筆')
                return [{
                    'code':  q.get('symbol','').replace('F.TW',''),
                    'name':  q.get('shortName') or q.get('longName',''),
                    'volume': safe_float(q.get('regularMarketVolume')),
                    'price':  safe_float(q.get('regularMarketPrice')),
                    'open_interest': None,
                } for q in quotes if q.get('regularMarketVolume')]
    except Exception as e:
        print(f'  [Screener錯誤] {e}')
    return []

# ── 計算量增排行 ──────────────────────────────────────────
def calc_ranking(today_recs, yest_recs):
    ymap = {(r.get('code') or r.get('name')): r for r in (yest_recs or [])}
    result = []
    for r in today_recs:
        key   = r.get('code') or r.get('name')
        yest  = ymap.get(key)
        vol_t = r.get('volume') or 0
        vol_y = (yest.get('volume') or 0) if yest else 0
        chg   = round((vol_t-vol_y)/vol_y*100, 2) if vol_y > 0 else None
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
    
    # 先試 screener，再試逐一查詢
    recs = fetch_via_screener()
    if not recs:
        recs = fetch_all_futures(date_str)
    
    if not recs:
        print(f'  → 無資料，跳過')
        return False
    
    save_json(os.path.join(DATA_DIR, f'raw_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'records': recs})
    
    yest_data = load_json(os.path.join(DATA_DIR, f'raw_{prev_str}.json')) if prev_str else None
    yest_recs = yest_data.get('records',[]) if yest_data else []
    ranking   = calc_ranking(recs, yest_recs)
    
    save_json(os.path.join(DATA_DIR, f'ranking_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'ranking': ranking})
    
    print(f'  → 存入 {len(recs)} 筆，排行前3：', end='')
    for r in ranking[:3]:
        p = f"{r['volume_change_pct']:+.1f}%" if r['volume_change_pct'] is not None else 'N/A'
        print(f'{r["name"]}({p})', end='  ')
    print()
    update_index(date_str)
    return True

def main():
    ensure_dir()
    print(f'\n{"="*50}\n 個股期貨爬蟲 v5  {now_str()}\n{"="*50}')
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
