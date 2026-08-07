import os
import re
import time
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
from yfinance import EquityQuery
from curl_cffi import requests as requests_cffi

app = Flask(__name__)
CORS(app)

# 차단 우회를 위한 브라우저 세션 생성 (Chrome 브라우저 위장, 해외 종목 조회에 사용)
session = requests_cffi.Session(impersonate="chrome110")

_PERIOD_DAYS = {'1mo': 45, '1y': 380, '5y': 1900}


# ---------------------------------------------------------
# KRX(한국거래소) 종목코드 -> 종목명 캐시 (국내 주식/ETF 이름 조회용)
# 코스닥 종목은 야후 파이낸스에서 심볼 뒤에 '.KQ'를 붙여야 하므로
# 시장구분(코스피/코스닥)도 함께 캐싱해둔다.
# ---------------------------------------------------------
_krx_name_cache = {}
_krx_market_cache = {}  # {'035720': 'KOSDAQ', ...} (코스닥 종목만 채워짐)
_krx_cache_lock = threading.Lock()
_krx_cache_updated_at = 0
_KRX_CACHE_TTL = 6 * 60 * 60  # 6시간마다 갱신


def get_krx_name_map():
    """{'005930': '삼성전자', ...} 형태의 딕셔너리를 반환 (캐시됨)"""
    global _krx_name_cache, _krx_market_cache, _krx_cache_updated_at
    now = time.time()

    if _krx_name_cache and (now - _krx_cache_updated_at) < _KRX_CACHE_TTL:
        return _krx_name_cache

    # 서버 기동 직후(콜드 스타트) 백그라운드 워밍업 스레드가 전체 종목 목록을 다운로드하는 동안
    # 락을 잡고 있을 수 있다. 이때 요청 스레드가 락 대기로 멈춰버리면
    # gunicorn/Render의 요청 타임아웃(약 30초)을 넘겨 연결이 끊길 수 있으므로,
    # 락 획득을 최대 8초까지만 시도하고 실패하면 빈 캐시로 즉시 넘어간다.
    # (3초는 콜드 스타트 직후 워밍업이 아직 안 끝난 경우가 많아 한국 종목명이
    # 영문/로마자로 표시되는 경우가 잦았음 -> 8초로 늘려 대부분의 경우 워밍업이
    # 끝날 시간을 확보. 그래도 실패하면 이름은 이후 get_foreign_name()으로
    # 폴백되므로 시세 조회 자체는 막히지 않는다.)
    acquired = _krx_cache_lock.acquire(timeout=8)
    if not acquired:
        return _krx_name_cache  # 워밍업 진행 중이면 빈 캐시(또는 이전 캐시) 그대로 반환

    try:
        now = time.time()
        if _krx_name_cache and (now - _krx_cache_updated_at) < _KRX_CACHE_TTL:
            return _krx_name_cache

        try:
            import FinanceDataReader as fdr
            new_map = {}
            new_market_map = {}

            df_stock = fdr.StockListing('KRX')
            codes = df_stock['Code'].astype(str).str.zfill(6)
            new_map.update(dict(zip(codes, df_stock['Name'])))
            if 'Market' in df_stock.columns:
                new_market_map.update(dict(zip(codes, df_stock['Market'])))

            try:
                df_etf = fdr.StockListing('ETF/KR')
                new_map.update(
                    dict(zip(df_etf['Symbol'].astype(str).str.zfill(6), df_etf['Name']))
                )
                # ETF는 대부분 코스피(.KS) 표기이므로 시장구분은 별도로 안 채움
            except Exception as e_etf:
                print(f"[ETF 목록 캐시 갱신 실패] {e_etf}")

            if new_map:
                _krx_name_cache = new_map
                _krx_market_cache = new_market_map
                _krx_cache_updated_at = now
        except Exception as e:
            print(f"[KRX 종목명 캐시 갱신 실패] {e}")

        return _krx_name_cache
    finally:
        _krx_cache_lock.release()


def get_yahoo_suffix(code: str) -> str:
    """국내 종목 코드에 붙일 야후 파이낸스 접미사를 시장구분에 따라 결정.
    코스닥이면 '.KQ', 그 외(코스피/ETF 등)는 '.KS'."""
    get_krx_name_map()  # 캐시 예열 보장
    market = _krx_market_cache.get(code, '')
    return '.KQ' if market == 'KOSDAQ' else '.KS'


# ---------------------------------------------------------
# 해외 종목/ETF 티커 -> 이름 캐시 (yfinance info 호출은 느리므로 캐싱)
# ---------------------------------------------------------
_foreign_name_cache = {}
_foreign_name_cache_lock = threading.Lock()
_FOREIGN_NAME_TTL = 24 * 60 * 60  # 24시간


def get_foreign_name(symbol: str, ticker: "yf.Ticker" = None):
    entry = _foreign_name_cache.get(symbol)
    if entry and (time.time() - entry['ts']) < _FOREIGN_NAME_TTL:
        return entry['name']

    with _foreign_name_cache_lock:
        entry = _foreign_name_cache.get(symbol)
        if entry and (time.time() - entry['ts']) < _FOREIGN_NAME_TTL:
            return entry['name']

        try:
            t = ticker or yf.Ticker(symbol, session=session)
            info = t.info
            name = info.get('longName') or info.get('shortName')
        except Exception as e:
            print(f"[해외 종목명 조회 실패] {symbol}: {e}")
            name = entry['name'] if entry else None

        _foreign_name_cache[symbol] = {'name': name, 'ts': time.time()}
        return name


# ---------------------------------------------------------
# 시세 조회 결과 캐시 (국내/해외 공용, 60초 TTL)
# ---------------------------------------------------------
_stock_cache = {}
_stock_cache_lock = threading.Lock()
_STOCK_CACHE_TTL = 60


def get_cached_stock(key: str):
    entry = _stock_cache.get(key)
    if entry and (time.time() - entry['ts']) < _STOCK_CACHE_TTL:
        return entry['data']
    return None


def set_cached_stock(key: str, data: dict):
    with _stock_cache_lock:
        _stock_cache[key] = {'data': data, 'ts': time.time()}


def stale_fallback(key: str):
    """실패 시 만료된 캐시라도 있으면 재사용 (stale 표시 추가)"""
    stale = _stock_cache.get(key)
    if stale:
        fb = dict(stale['data'])
        fb['stale'] = True
        return fb
    return None


def resolve_period(raw: str) -> str:
    p = (raw or '1mo').strip().lower()
    return p if p in _PERIOD_DAYS else '1mo'


def fetch_yahoo_quote(yahoo_symbol: str, period: str) -> dict:
    """야후 파이낸스에서 가격/차트/52주 고저/시가총액 등을 조회.
    name은 포함하지 않음 (호출부에서 소스에 맞게 채움)."""
    ticker = yf.Ticker(yahoo_symbol, session=session)
    fast_info = ticker.fast_info

    current_price = fast_info.last_price
    previous_close = fast_info.previous_close

    if current_price is None:
        raise ValueError("데이터 없음")

    change = current_price - previous_close if previous_close else 0
    change_percent = (change / previous_close) * 100 if previous_close else 0

    history = ticker.history(period=period)
    chart_data = [
        {"date": date.strftime('%Y-%m-%d'), "close": round(row['Close'], 2)}
        for date, row in history.iterrows()
    ]

    return {
        'currentPrice': current_price,
        'previousClose': previous_close,
        'change': change,
        'changePercent': change_percent,
        'currency': fast_info.currency or 'USD',
        'high52': fast_info.year_high,
        'low52': fast_info.year_low,
        'marketCap': fast_info.market_cap,
        'chart': chart_data,
        '_ticker': ticker,  # 이름 조회에 재사용 (응답에는 포함하지 않음)
    }


# ---------------------------------------------------------
# 국내 주식/ETF 전용: 종목명은 KRX 캐시를 우선 사용하되,
# 캐시에 없으면(최근 상장 등) 야후 파이낸스 이름으로 폴백한다.
# 가격/차트 등 나머지 시세 정보는 전부 야후 파이낸스에서 가져온다.
# ---------------------------------------------------------
_KR_CODE_RE = re.compile(r'^[A-Z0-9]{6}$')


def is_kr_code(code: str) -> bool:
    """국내 종목/ETF 코드 형식: 6자리이고 숫자를 최소 1개 이상 포함
    (예: '005930', 최근 상장된 단일종목 ETF의 '0195S0' 같은 코드도 허용)."""
    return bool(_KR_CODE_RE.match(code)) and any(ch.isdigit() for ch in code)


@app.route('/api/kr-stock')
def get_kr_stock():
    code = request.args.get('code', '').strip().upper()
    period = resolve_period(request.args.get('period'))

    if not is_kr_code(code):
        return jsonify({'error': '국내 종목 코드는 6자리 코드여야 합니다.'}), 400

    cache_key = f"KR:{code}:{period}"
    cached = get_cached_stock(cache_key)
    if cached:
        return jsonify(cached)

    yahoo_symbol = code + get_yahoo_suffix(code)

    try:
        quote = fetch_yahoo_quote(yahoo_symbol, period)
        ticker = quote.pop('_ticker')

        # 1순위: KRX 종목명 캐시. 없으면(신규 상장 등) 야후 이름으로 폴백.
        name = get_krx_name_map().get(code) or get_foreign_name(yahoo_symbol, ticker)

        result = {
            'symbol': code,
            'name': name,
            **quote,
        }
        set_cached_stock(cache_key, result)
        return jsonify(result)

    except Exception as e:
        fallback = stale_fallback(cache_key)
        if fallback:
            return jsonify(fallback)
        return jsonify({'error': f'국내 시세 조회 실패: ({str(e)})'}), 404


# ---------------------------------------------------------
# 해외 주식/ETF 전용: 종목명, 시세 모두 야후 파이낸스에서 가져온다.
# 프론트엔드가 6자리 숫자가 아닌 티커(AAPL, KRW=X 등)일 때 이 엔드포인트를 호출한다.
# ---------------------------------------------------------
@app.route('/api/global-stock')
def get_global_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    period = resolve_period(request.args.get('period'))

    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    cache_key = f"G:{symbol}:{period}"
    cached = get_cached_stock(cache_key)
    if cached:
        return jsonify(cached)

    try:
        quote = fetch_yahoo_quote(symbol, period)
        ticker = quote.pop('_ticker')

        result = {
            'symbol': symbol,
            'name': get_foreign_name(symbol, ticker),
            **quote,
        }
        set_cached_stock(cache_key, result)
        return jsonify(result)

    except Exception as e:
        fallback = stale_fallback(cache_key)
        if fallback:
            return jsonify(fallback)
        return jsonify({'error': f'해외 시세 조회 실패: ({str(e)})'}), 404


# ---------------------------------------------------------
# 시가총액 상위 스크리너 (미국/한국) - 야후 EquityQuery 사용
# 하드코딩된 종목 리스트 없이 야후가 계산한 시가총액 기준으로
# 실시간 상위 종목을 직접 조회한다.
# ---------------------------------------------------------
_SCREENER_CACHE_TTL = 300  # 5분 (요청마다 새로 스크리닝하면 느리고 차단 위험도 있음)
_screener_cache = {}
_screener_cache_lock = threading.Lock()


def get_cached_screener(key: str):
    entry = _screener_cache.get(key)
    if entry and (time.time() - entry['ts']) < _SCREENER_CACHE_TTL:
        return entry['data']
    return None


def set_cached_screener(key: str, data: dict):
    with _screener_cache_lock:
        _screener_cache[key] = {'data': data, 'ts': time.time()}


def screener_stale_fallback(key: str):
    """스크리너 실패 시 만료된 캐시라도 있으면 재사용 (stale 표시 추가)"""
    stale = _screener_cache.get(key)
    if stale:
        fb = dict(stale['data'])
        fb['stale'] = True
        return fb
    return None


def run_marketcap_screen(region: str, offset: int, count: int):
    """(중복 제거된 종목 리스트, 야후가 실제로 내려준 원본 개수)를 반환.
    dedup으로 개수가 줄어들 수 있으므로, '다음 페이지가 더 있는지' 판단은
    dedup 이전의 원본 개수를 기준으로 해야 한다."""
    q = EquityQuery('and', [
        EquityQuery('eq', ['region', region]),
        EquityQuery('gt', ['intradaymarketcap', 0]),
    ])
    result = yf.screen(q, sortField='intradaymarketcap', sortAsc=False, offset=offset, size=count)
    quotes = result.get('quotes', []) if result else []
    raw_count = len(quotes)

    items = []
    for item in quotes:
        raw_name = item.get('longName') or item.get('shortName') or item.get('symbol') or ''
        items.append({
            'symbol': item.get('symbol'),
            'name': raw_name,
            # 같은 회사의 보통주/우선주, 복수 클래스 주식(GOOGL/GOOG 등)을 하나로 묶기 위한 키.
            # 야후는 이런 경우 longName이 보통 동일하게 내려오므로 이를 그대로 그룹핑 기준으로 쓴다.
            '_group_key': re.sub(r'\s+', ' ', raw_name).strip().lower() or item.get('symbol'),
            'exchange': item.get('fullExchangeName') or item.get('exchange'),
            'currency': item.get('currency'),
            'price': item.get('regularMarketPrice'),
            'change': item.get('regularMarketChange'),
            'changePercent': item.get('regularMarketChangePercent'),
            'marketCap': item.get('marketCap'),
        })

    # 이미 시가총액 내림차순으로 정렬돼 있으므로, 같은 그룹에서 먼저 나오는(=시가총액이 더 큰)
    # 종목만 남기고 나머지(우선주, 보조 클래스 등)는 제거한다.
    seen_groups = set()
    deduped = []
    for it in items:
        key = it['_group_key']
        if key in seen_groups:
            continue
        seen_groups.add(key)
        deduped.append(it)

    if region == 'kr':
        krx_names = get_krx_name_map()
        for it in deduped:
            bare_code = (it['symbol'] or '').split('.')[0]
            korean_name = krx_names.get(bare_code)
            if korean_name:
                it['name'] = korean_name

    for it in deduped:
        it.pop('_group_key', None)

    return deduped, raw_count


@app.route('/api/top-marketcap')
def get_top_marketcap():
    region = request.args.get('region', 'us').strip().lower()
    if region not in ('us', 'kr'):
        return jsonify({'error': "region은 'us' 또는 'kr'만 지원합니다."}), 400

    try:
        count = int(request.args.get('count', 100))
    except ValueError:
        count = 100
    count = max(1, min(count, 250))  # 야후 스크리너 1회 최대 250개

    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0
    offset = max(0, offset)

    cache_key = f"SCREEN:{region}:{offset}:{count}"
    cached = get_cached_screener(cache_key)
    if cached:
        return jsonify(cached)

    try:
        items, raw_count = run_marketcap_screen(region, offset, count)
        if raw_count == 0:
            return jsonify({'error': f'{region} 시가총액 스크리너 결과가 비어있습니다.'}), 404

        result = {
            'region': region,
            'offset': offset,
            'count': len(items),
            'rawCount': raw_count,  # 중복 제거 전 원본 개수. 다음 페이지 존재 여부 판단용(rawCount == count면 더 있을 가능성 높음)
            'items': items,
        }
        set_cached_screener(cache_key, result)
        return jsonify(result)
    except Exception as e:
        fallback = screener_stale_fallback(cache_key)
        if fallback:
            return jsonify(fallback)
        return jsonify({'error': f'시가총액 순위 조회 실패: ({str(e)})'}), 502


# 서버 시작 시 백그라운드에서 KRX 종목명 캐시를 미리 예열
threading.Thread(target=get_krx_name_map, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
