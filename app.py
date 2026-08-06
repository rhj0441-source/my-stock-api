import os
import time
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
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

    with _krx_cache_lock:
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
# 국내 주식/ETF 전용: 종목명은 KRX 캐시에서, 나머지 시세 정보는
# 전부 야후 파이낸스에서 가져온다.
# ---------------------------------------------------------
@app.route('/api/kr-stock')
def get_kr_stock():
    code = request.args.get('code', '').strip()
    period = resolve_period(request.args.get('period'))

    if not (code.isdigit() and len(code) == 6):
        return jsonify({'error': '국내 종목 코드는 6자리 숫자여야 합니다.'}), 400

    cache_key = f"KR:{code}:{period}"
    cached = get_cached_stock(cache_key)
    if cached:
        return jsonify(cached)

    yahoo_symbol = code + get_yahoo_suffix(code)

    try:
        quote = fetch_yahoo_quote(yahoo_symbol, period)
        quote.pop('_ticker', None)

        result = {
            'symbol': code,
            'name': get_krx_name_map().get(code),  # 종목명만 KRX 캐시에서
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


# 서버 시작 시 백그라운드에서 KRX 종목명 캐시를 미리 예열
threading.Thread(target=get_krx_name_map, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
