#!/usr/bin/env python3
"""
個股期貨資料爬蟲 v6
逐一查詢 Yahoo Finance 個股期貨歷史成交量
台灣個股期貨代碼格式：{股票代碼}F.TWO 或 {股票代碼}F.TW
"""

import json, os, sys, time, datetime, requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TOP_N    = 20

# 台灣主要個股期貨完整清單
FUTURES_LIST = [
    ('2330','台積電'),('2317','鴻海'),('2454','聯發科'),
    ('2382','廣達'),('2412','中華電'),('2308','台達電'),
    ('2881','富邦金'),('2882','國泰金'),('2886','兆豐金'),
    ('2891','中信金'),('2884','玉山金'),('2892','第一金'),
    ('2303','聯電'),('2344','華邦電'),('3711','日月光投控'),
    ('2379','瑞昱'),('2357','華碩'),('2395','研華'),
    ('3034','聯詠'),('2327','國巨'),('2376','技嘉'),
    ('2474','可成'),('4904','遠傳'),('2609','陽明'),
    ('2615','萬海'),('2603','長榮'),('1301','台塑'),
    ('1303','南亞'),('1326','台化'),('2002','中鋼'),
    ('5871','中租控股'),('2207','和泰車'),('6505','台塑化'),
    ('2912','統一超'),('2801','彰銀'),('5880','合庫金'),
    ('2883','開發金'),('2885','元大金'),('2887','台新金'),
    ('2888','新光金'),('3008','大立光'),('3045','台灣大'),
    ('2408','南亞科'),('2360','致茂'),('2301','光寶科'),
    ('2347','聯強'),('2353','宏碁'),('2356','英業達'),
    ('2358','廷鑫'),('2360','致茂'),('2377','微星'),
    ('2385','群光'),('2392','正崴'),('2409','友達'),
    ('2448','晶電'),('2449','京元電子'),('2450','神腦'),
    ('2451','創見'),('2458','義隆'),('2461','光群雷'),
    ('2492','華新科'),('2498','宏達電'),('2511','太子'),
    ('2545','皇翔'),('2548','華固'),('2601','益航'),
    ('2605','新興'),('2606','裕民'),('2607','榮運'),
    ('2610','華航'),('2618','長榮航'),('2633','台灣高鐵'),
    ('2801','彰銀'),('2809','京城銀'),('2812','台中銀'),
    ('2823','中壽'),('2832','台產'),('2834','臺企銀'),
    ('2836','高雄銀'),('2838','聯邦銀'),('2845','遠東銀'),
    ('2849','安泰銀'),('2850','新產'),('2851','中再保'),
    ('2852','第一保'),('2855','統一證'),('2856','元富證'),
    ('2867','三商壽'),('2880','華南金'),('2889','國票金'),
    ('2890','永豐金'),('2893','王道銀'),('3006','晶豪科'),
    ('3009','奇美電'),('3010','華立'),('3013','晟銘電'),
    ('3015','奇力新'),('3017','奇鋐'),('3018','同開'),
    ('3019','亞光'),('3020','佐登'),('3022','威強電'),
    ('3023','信邦'),('3024','憶聲'),('3025','星通'),
    ('3026','禾伸堂'),('3027','盛達'),('3028','通毅'),
    ('3029','零壹'),('3030','晶相光'),('3031','佰鴻'),
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json,text/html,*/*',
    'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
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
    dt = datetime.datetime.strptime(date_str,'%Y-%m-%d').replace(tzinfo=tz)
    return int(dt.timestamp())

# ── Yahoo Finance 查詢單一個股期貨的歷史成交量 ────────────
def fetch_one(code, name, date_str):
    """
    查詢特定日期的個股期貨成交量
    用 v8/finance/chart 抓日線歷史資料
    """
    # 計算查詢區間（前後各1天，確保能抓到該日資料）
    tz     = datetime.timezone(datetime.timedelta(hours=8))
    dt     = datetime.datetime.strptime(date_str,'%Y-%m-%d').replace(tzinfo=tz)
    period1 = int((dt - datetime.timedelta(days=1)).timestamp())
    period2 = int((dt + datetime.timedelta(days=2)).timestamp())

    for suffix in ['F.TW', 'F.TWO']:
        symbol = f'{code}{suffix}'
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
        params = {
            'period1': period1,
            'period2': period2,
            'interval': '1d',
            'events': 'history',
        }
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            result = data.get('chart',{}).get('result',[])
            if not result:
                continue

            r          = result[0]
            timestamps = r.get('timestamp', [])
            indicators = r.get('indicators', {})
            quotes     = indicators.get('quote', [{}])[0]
            volumes    = quotes.get('volume', [])
            closes     = quotes.get('close', [])

            # 找到目標日期的資料
            target_date = dt.date()
            for i, ts in enumerate(timestamps):
                ts_date = datetime.datetime.fromtimestamp(ts, tz=tz).date()
                if ts_date == target_date:
                    vol   = volumes[i] if i < len(volumes) else None
                    price = closes[i]  if i < len(closes)  else None
                    if vol and vol > 0:
                        return {
                            'code': code, 'name': name,
                            'volume': float(vol), 'price': price,
                            'open_interest': None,
                        }
            # 找不到目標日期，嘗試取最近一筆
            if volumes:
                last_vol = next((v for v in reversed(volumes) if v), None)
                last_price = next((c for c in reversed(closes) if c), None)
                if last_vol and last_vol > 0:
                    return {
                        'code': code, 'name': name,
                        'volume': float(last_vol), 'price': last_price,
                        'open_interest': None,
                    }
        except Exception:
            continue
    return None

# ── 批次查詢所有個股期貨 ──────────────────────────────────
def fetch_all(date_str):
    print(f'  開始查詢 {len(FUTURES_LIST)} 檔個股期貨...')
    records = []
    found   = 0
    for i, (code, name) in enumerate(FUTURES_LIST):
        result = fetch_one(code, name, date_str)
        if result:
            records.append(result)
            found += 1
            if found % 5 == 0:
                print(f'  已找到 {found} 筆... ({i+1}/{len(FUTURES_LIST)})')
        time.sleep(0.2)

    print(f'  查詢完成：{len(FUTURES_LIST)} 檔中找到 {len(records)} 筆有效資料')
    return records

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
    recs = fetch_all(date_str)

    if not recs:
        print(f'  → 無資料，跳過（可能為非交易日）')
        return False

    save_json(os.path.join(DATA_DIR, f'raw_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'records': recs})

    yest_data = load_json(os.path.join(DATA_DIR, f'raw_{prev_str}.json')) if prev_str else None
    yest_recs = yest_data.get('records',[]) if yest_data else []
    ranking   = calc_ranking(recs, yest_recs)

    save_json(os.path.join(DATA_DIR, f'ranking_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'ranking': ranking})

    print(f'  → 存入 {len(recs)} 筆，排行前5：')
    for i, r in enumerate(ranking[:5], 1):
        p = f"{r['volume_change_pct']:+.1f}%" if r['volume_change_pct'] is not None else 'N/A'
        print(f'     {i}. {r["name"]}  成交量:{r.get("volume")}  量增:{p}')
    update_index(date_str)
    return True

def main():
    ensure_dir()
    print(f'\n{"="*50}\n 個股期貨爬蟲 v6  {now_str()}\n{"="*50}')
    bs = os.environ.get('BACKFILL_START','').strip()
    be = os.environ.get('BACKFILL_END','').strip()

    if bs and be:
        dates = get_weekdays(bs, be)
        print(f'[補抓] {bs} ~ {be}，共 {len(dates)} 天')
        for i, d in enumerate(dates):
            process_date(d, dates[i-1] if i > 0 else None)
            time.sleep(2)
    else:
        today = today_str()
        tz    = datetime.timezone(datetime.timedelta(hours=8))
        yest  = (datetime.datetime.now(tz)-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(f'[每日] {today}')
        process_date(today, yest)

    print(f'\n✅ 完成！\n')

if __name__ == '__main__':
    main()
