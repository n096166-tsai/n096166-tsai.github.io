#!/usr/bin/env python3
"""
個股期貨資料爬蟲 v4
使用台灣期交所官網可連線的 API
"""

import json, os, sys, time, datetime, requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TOP_N    = 20

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.taifex.com.tw/',
}

def ensure_dir():   os.makedirs(DATA_DIR, exist_ok=True)

def tw_now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))

def today_str():    return tw_now().strftime('%Y-%m-%d')
def now_str():      return tw_now().strftime('%Y-%m-%d %H:%M')

def load_json(path):
    return json.load(open(path, encoding='utf-8')) if os.path.exists(path) else None

def save_json(path, data):
    json.dump(data, open(path, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)

def safe_float(s):
    try:    return float(str(s).replace(',','').replace('+','').strip())
    except: return None

def get_weekdays(start_str, end_str):
    tz  = datetime.timezone(datetime.timedelta(hours=8))
    s   = datetime.datetime.strptime(start_str, '%Y-%m-%d').replace(tzinfo=tz)
    e   = datetime.datetime.strptime(end_str,   '%Y-%m-%d').replace(tzinfo=tz)
    days, cur = [], s
    while cur <= e:
        if cur.weekday() < 5: days.append(cur.strftime('%Y-%m-%d'))
        cur += datetime.timedelta(days=1)
    return days

# ── API：期交所官網 AJAX（個股期貨日交易資料）────────────
def fetch_taifex(date_str):
    """
    使用期交所官網的查詢介面 API
    date_str: '2026-03-23'
    """
    date_slash = date_str.replace('-', '/')

    # 方法一：期交所官網查詢 API
    urls = [
        # 個股期貨日行情查詢
        f'https://www.taifex.com.tw/cht/3/futDataDown?down_type=1'
        f'&queryStartDate={date_slash}&queryEndDate={date_slash}&commodity_id=SF',
        # 備用：全商品
        f'https://www.taifex.com.tw/cht/3/futDataDown?down_type=1'
        f'&queryStartDate={date_slash}&queryEndDate={date_slash}&commodity_id=',
    ]

    for url in urls:
        print(f'  [嘗試] {url[:80]}...')
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            print(f'  [HTTP] {resp.status_code}  大小:{len(resp.content)} bytes')
            if resp.status_code != 200 or len(resp.content) < 100:
                continue

            # 解碼
            for enc in ('big5', 'utf-8-sig', 'utf-8', 'cp950'):
                try:
                    text = resp.content.decode(enc)
                    break
                except: continue
            else:
                continue

            records = []
            lines   = [l.strip() for l in text.strip().split('\n') if l.strip()]
            print(f'  [解析] {len(lines)} 行')

            for line in lines:
                cols = [c.strip().strip('"') for c in line.split(',')]
                if len(cols) < 8: continue
                # 跳過標頭行
                if any(h in cols[0] for h in ['日期','交易日','Date']): continue
                # 跳過非個股期貨（商品代碼含數字的才是個股期貨）
                name = cols[1] if len(cols) > 1 else ''
                vol  = safe_float(cols[9]) if len(cols) > 9 else None
                if not name or vol is None or vol == 0: continue

                price = safe_float(cols[6]) if len(cols) > 6 else None
                oi    = safe_float(cols[11]) if len(cols) > 11 else None
                records.append({
                    'code': cols[1], 'name': name,
                    'volume': vol, 'price': price, 'open_interest': oi,
                })

            if records:
                print(f'  [成功] 解析 {len(records)} 筆')
                return records
            print(f'  [空] 解析結果為空')
        except Exception as e:
            print(f'  [錯誤] {e}')

    # 方法二：期交所 JSON 查詢（另一端點）
    json_urls = [
        f'https://www.taifex.com.tw/cht/3/getFuturesDataByDate?queryDate={date_slash}&commodity_id=SF',
        f'https://www.taifex.com.tw/api/v1/dailyMarketInfo?date={date_str.replace("-","")}&type=futures',
    ]
    for url in json_urls:
        print(f'  [JSON] {url[:80]}...')
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200 and len(resp.content) > 50:
                data = resp.json()
                print(f'  [JSON] 回傳類型: {type(data).__name__}')
                if isinstance(data, list) and data:
                    records = []
                    for row in data:
                        vol  = safe_float(row.get('volume') or row.get('Volume') or row.get('成交量'))
                        name = str(row.get('name') or row.get('Name') or row.get('contractName') or '')
                        if name and vol:
                            records.append({
                                'code': row.get('code',''), 'name': name,
                                'volume': vol, 'price': safe_float(row.get('close')),
                                'open_interest': safe_float(row.get('openInterest')),
                            })
                    if records:
                        return records
        except Exception as e:
            print(f'  [JSON錯誤] {e}')

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
        chg   = round((vol_t - vol_y) / vol_y * 100, 2) if vol_y > 0 else None
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
    recs = fetch_taifex(date_str)
    if not recs:
        print(f'  → 無資料，跳過')
        return False
    save_json(os.path.join(DATA_DIR, f'raw_{date_str}.json'),
              {'date': date_str, 'fetched': now_str(), 'records': recs})
    yest_data = load_json(os.path.join(DATA_DIR, f'raw_{prev_str}.json')) if prev_str else None
    yest_recs = yest_data.get('records', []) if yest_data else []
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
    print(f'\n{"="*50}\n 個股期貨爬蟲 v4  {now_str()}\n{"="*50}')
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
