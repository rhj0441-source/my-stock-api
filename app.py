import os
import re
import time
import threading
import concurrent.futures
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
from yfinance import EquityQuery
from curl_cffi import requests as requests_cffi

app = Flask(__name__)
CORS(app)

# 차단 우회를 위한 브라우저 세션 생성 (Chrome 브라우저 위장, 해외 종목 조회에 사용)
session = requests_cffi.Session(impersonate="chrome110")

# ---------------------------------------------------------
# 야후 파이낸스 호출 스로틀링 (Too Many Requests / 429 방지)
# 여러 요청(즐겨찾기, 지수 위젯 등)이 동시에 몰리면 짧은 시간에 야후로 나가는
# 호출이 급증해 IP 단위로 레이트리밋(429)에 걸리기 쉽다. 앱 전체에서 공유하는
# 최소 호출 간격을 두어 순간적으로 몰리는 요청을 자연스럽게 줄지어 세운다.
# ---------------------------------------------------------
_yahoo_call_lock = threading.Lock()
_yahoo_last_call_ts = 0.0
_YAHOO_MIN_INTERVAL = 0.4  # 최소 호출 간격(초). 대략 초당 2~3회로 제한.


def _throttle_yahoo_call():
    global _yahoo_last_call_ts
    with _yahoo_call_lock:
        now = time.time()
        wait = _YAHOO_MIN_INTERVAL - (now - _yahoo_last_call_ts)
        if wait > 0:
            time.sleep(wait)
        _yahoo_last_call_ts = time.time()


def _is_rate_limit_error(e: Exception) -> bool:
    msg = str(e)
    return 'Too Many Requests' in msg or 'Rate limited' in msg or '429' in msg


def _yahoo_call_with_retry(fn, *args, retries=2, backoff=2.5, **kwargs):
    """야후 API 호출을 스로틀링 + 429(Too Many Requests) 발생 시 자동 재시도로 감싼다."""
    last_err = None
    wait = backoff
    for attempt in range(retries + 1):
        _throttle_yahoo_call()
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            if _is_rate_limit_error(e) and attempt < retries:
                print(f"[야후 레이트리밋] {attempt + 1}번째 시도 실패, {wait:.1f}초 대기 후 재시도")
                time.sleep(wait)
                wait *= 1.8
                continue
            raise
    raise last_err

# 프론트엔드 기간 버튼 값 -> yfinance가 실제로 받는 period 문자열 매핑
# ('1w'는 yfinance에 없는 값이라 가장 가까운 '5d'로 매핑)
_PERIOD_MAP = {
    '1w': '5d',
    '1mo': '1mo',
    '1y': '1y',
    '5y': '5y',
    '10y': '10y',
    'max': 'max',
}


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
    # 락 획득 대기시간을 상황별로 다르게 둔다.
    # - 최초 예열(이전에 한 번도 캐시가 채워진 적 없음): 여기서 빈 캐시로 넘어가면
    #   실패한 이름이 스크리너의 5분 캐시에 그대로 박제되는 문제가 있었으므로,
    #   gunicorn 타임아웃(~30초) 안에서 최대한 예열이 끝나길 기다린다(25초).
    # - 이후 정기 갱신(6시간마다, 이미 이전 캐시가 있는 상태): 사용자를 오래 기다리게
    #   할 필요 없이 8초만 기다리고 실패하면 기존(이전) 캐시를 그대로 반환한다.
    is_cold_start = not _krx_name_cache
    acquired = _krx_cache_lock.acquire(timeout=25 if is_cold_start else 8)
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
                # 덮어쓰기(=) 대신 병합(update)한다. resolve_missing_kr_name()으로
                # 개별 보강해둔 종목명이 있다면, 벌크 소스가 6시간마다(또는 서버 재시작마다)
                # 갱신될 때 그 값을 지워버리면 매번 느린 개별 재조회가 반복되기 때문.
                # 벌크 소스 값이 더 최신이므로 동일 코드가 있으면 벌크 값으로 덮인다.
                _krx_name_cache.update(new_map)
                _krx_market_cache.update(new_market_map)
                _krx_cache_updated_at = now
        except Exception as e:
            print(f"[KRX 종목명 캐시 갱신 실패] {e}")

        # FDR의 KRX 조회는 한국거래소 사이트 구조가 바뀔 때마다 종종 깨지는(404/401 등)
        # 고질적인 문제가 있다. FDR이 실패했거나 빈 결과를 줬다면, 한국거래소의
        # 다른 다운로드 창구(KIND 상장법인목록)를 대체 소스로 사용해본다.
        # 이 창구는 marketType별로 따로 요청할 수 있어 코스피/코스닥 구분도 같이 얻을 수 있다.
        if not _krx_name_cache:
            try:
                import pandas as pd
                kind_url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
                kind_name_map = {}
                kind_market_map = {}
                for market_type, market_label in (('stockMkt', 'KOSPI'), ('kosdaqMkt', 'KOSDAQ')):
                    df = pd.read_html(f'{kind_url}&marketType={market_type}', header=0)[0]
                    codes_k = df['종목코드'].astype(str).str.zfill(6)
                    kind_name_map.update(dict(zip(codes_k, df['회사명'])))
                    if market_label == 'KOSDAQ':
                        kind_market_map.update({c: 'KOSDAQ' for c in codes_k})

                if kind_name_map:
                    _krx_name_cache = kind_name_map
                    _krx_market_cache = kind_market_map
                    _krx_cache_updated_at = now
                    print(f"[KRX 종목명 캐시] FDR 실패 -> KIND 대체 소스로 {len(kind_name_map)}개 종목명 확보")
            except Exception as e2:
                print(f"[KRX 종목명 캐시 갱신 실패 - KIND 대체소스도 실패] {e2}")

        return _krx_name_cache
    finally:
        _krx_cache_lock.release()


def resolve_missing_kr_name(code: str):
    """FDR/KIND 벌크 목록에 없는 코드(스핀오프/홀딩스 등 특수 코드, 최근 상장,
    데이터 소스 누락 등)를 개별적으로 보강 조회한다.
    pykrx는 KRX를 직접 소스로 쓰므로 FDR/KIND와 커버리지가 달라
    벌크 목록에서 빠진 종목도 잡히는 경우가 많다.
    성공하면 전역 캐시에 영구 반영해 다음부터는 벌크 캐시처럼 즉시 반환된다."""
    global _krx_name_cache
    if code in _krx_name_cache:
        return _krx_name_cache[code]
    try:
        from pykrx import stock as pykrx_stock
        name = pykrx_stock.get_market_ticker_name(code)
        if name:
            with _krx_cache_lock:
                _krx_name_cache[code] = name
            return name
    except Exception as e:
        print(f"[개별 종목명 보강 실패] {code}: {e}")
    return None


def fetch_kr_fundamentals_pykrx(code: str) -> dict:
    """야후(quoteSummary)에서 PER/PBR/배당수익률을 못 가져왔을 때
    KRX 원천 데이터를 쓰는 pykrx로 보강 조회한다.
    pykrx 기본 API에는 ROE/부채비율이 없어 PER/PBR/배당수익률만 채운다."""
    try:
        from pykrx import stock as pykrx_stock
        from datetime import datetime, timedelta

        # 휴장일(주말/공휴일) 대비 최근 영업일을 최대 3일 전까지 역순으로 탐색
        # (너무 길게 탐색하면 실패 시 응답이 느려지므로 범위를 짧게 유지)
        for days_back in range(3):
            date_str = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            df = pykrx_stock.get_market_fundamental(date_str, date_str, code)
            if df is None or df.empty:
                continue
            row = df.iloc[-1]
            per = row.get('PER')
            pbr = row.get('PBR')
            div = row.get('DIV')
            return {
                'per': round(float(per), 2) if per not in (None, 0) and per == per else None,
                'pbr': round(float(pbr), 2) if pbr not in (None, 0) and pbr == pbr else None,
                'dividendYield': round(float(div), 2) if div is not None and div == div and div != 0 else None,
            }
    except Exception as e:
        print(f"[pykrx 재무지표 보강 실패] {code}: {e}")
    return {}


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
            info = _yahoo_call_with_retry(lambda: t.info)
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
    return p if p in _PERIOD_MAP else '1mo'


def _is_nan(v):
    return v is None or v != v  # NaN은 자기 자신과 같지 않다는 성질을 이용 (pandas 미의존)


def _pct(v):
    """0.245 같은 소수 비율을 24.5(%) 형태로 변환."""
    if _is_nan(v):
        return None
    return round(float(v) * 100, 2)


def fetch_fundamentals(ticker: "yf.Ticker") -> dict:
    """PER/PBR/ROE/부채비율/배당수익률 등 재무 지표 조회 (ticker.info 사용).
    종목에 따라 일부 지표가 아예 없을 수 있어(ETF 등) 개별적으로 None 처리.
    ticker.info가 쓰는 야후의 quoteSummary API는 가격/차트에 쓰이는 chart API보다
    훨씬 자주 차단/타임아웃되므로(특히 클라우드 서버 IP), 스로틀링 + 429 재시도를 적용한다."""
    try:
        info = _yahoo_call_with_retry(lambda: ticker.info or {})
    except Exception as e:
        print(f"[재무지표 조회 실패] {ticker.ticker}: {e}")
        info = {}

    if not info:
        return {'per': None, 'pbr': None, 'roe': None, 'debtRatio': None, 'dividendYield': None}

    per = info.get('trailingPE')
    if _is_nan(per):
        per = info.get('forwardPE')

    pbr = info.get('priceToBook')

    # dividendYield는 yfinance 버전에 따라 0.012(소수) 또는 1.2(이미 %) 두 형태로 내려온 이력이 있어
    # 값이 1.5보다 크면 이미 %로 간주하고 그대로 쓰고, 아니면 소수로 보고 100을 곱한다.
    raw_div_yield = info.get('dividendYield')
    if _is_nan(raw_div_yield):
        dividend_yield = None
    elif float(raw_div_yield) > 1.5:
        dividend_yield = round(float(raw_div_yield), 2)
    else:
        dividend_yield = _pct(raw_div_yield)

    return {
        'per': None if _is_nan(per) else round(float(per), 2),
        'pbr': None if _is_nan(pbr) else round(float(pbr), 2),
        'roe': _pct(info.get('returnOnEquity')),
        # debtToEquity는 Yahoo에서 이미 %로 내려옴 (예: 45.3 = 45.3%)
        'debtRatio': None if _is_nan(info.get('debtToEquity')) else round(float(info.get('debtToEquity')), 1),
        'dividendYield': dividend_yield,
    }


def fetch_annual_financials(ticker: "yf.Ticker") -> list:
    """최근 4개년 매출액/영업이익 조회 (연간 손익계산서 기준).
    ETF 등 손익계산서가 없는 종목은 빈 리스트를 반환한다."""
    try:
        fin = _yahoo_call_with_retry(lambda: ticker.financials)
        if fin is None or fin.empty:
            return []

        revenue_row = fin.loc['Total Revenue'] if 'Total Revenue' in fin.index else None
        op_income_row = fin.loc['Operating Income'] if 'Operating Income' in fin.index else None
        if revenue_row is None and op_income_row is None:
            return []

        # 컬럼(연도)은 최신순으로 내려오므로 4개만 취하고, 차트 표시를 위해 오래된 순으로 뒤집는다.
        cols = list(fin.columns)[:4]
        cols = list(reversed(cols))

        result = []
        for col in cols:
            year_label = col.strftime('%Y') if hasattr(col, 'strftime') else str(col)
            rev = revenue_row[col] if revenue_row is not None and col in revenue_row.index else None
            op = op_income_row[col] if op_income_row is not None and col in op_income_row.index else None
            result.append({
                'year': year_label,
                'revenue': None if _is_nan(rev) else float(rev),
                'operatingIncome': None if _is_nan(op) else float(op),
            })
        return result
    except Exception as e:
        print(f"[연간 실적 조회 실패] {ticker.ticker}: {e}")
        return []


def fetch_yahoo_quote(yahoo_symbol: str, period: str, light: bool = False) -> dict:
    """야후 파이낸스에서 가격/차트/52주 고저/시가총액/재무지표/연간 실적을 조회.
    name은 포함하지 않음 (호출부에서 소스에 맞게 채움).
    light=True면 PER/PBR/ROE 등 재무지표와 연간 실적 조회를 건너뛴다.
    지수(코스피/나스닥 등)처럼 애초에 재무지표가 없는 대상이나, 위젯처럼 가격만
    빠르게 필요한 경우 야후 호출 횟수를 줄여 응답 속도를 크게 개선한다."""
    ticker = yf.Ticker(yahoo_symbol, session=session)

    def _read_fast_info():
        fi = ticker.fast_info
        return fi.last_price, fi.previous_close, fi.currency, fi.year_high, fi.year_low, fi.market_cap

    current_price, previous_close, currency, year_high, year_low, market_cap = _yahoo_call_with_retry(_read_fast_info)

    if current_price is None:
        raise ValueError("데이터 없음")

    change = current_price - previous_close if previous_close else 0
    change_percent = (change / previous_close) * 100 if previous_close else 0

    history = _yahoo_call_with_retry(ticker.history, period=_PERIOD_MAP.get(period, '1mo'))
    chart_data = [
        {"date": date.strftime('%Y-%m-%d'), "close": round(row['Close'], 2)}
        for date, row in history.iterrows()
    ]

    if light:
        fundamentals = {'per': None, 'pbr': None, 'roe': None, 'debtRatio': None, 'dividendYield': None}
        financials = []
    else:
        fundamentals = fetch_fundamentals(ticker)
        financials = fetch_annual_financials(ticker)

    return {
        'currentPrice': current_price,
        'previousClose': previous_close,
        'change': change,
        'changePercent': change_percent,
        'currency': currency or 'USD',
        'high52': year_high,
        'low52': year_low,
        'marketCap': market_cap,
        'chart': chart_data,
        **fundamentals,
        'financials': financials,
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

        # 1순위: KRX 종목명 벌크 캐시. 2순위: 개별 보강 조회(pykrx).
        # 그래도 없으면(신규 상장 등) 야후 이름으로 최종 폴백.
        name = get_krx_name_map().get(code) or resolve_missing_kr_name(code) or get_foreign_name(yahoo_symbol, ticker)

        result = {
            'symbol': code,
            'name': name,
            **quote,
        }

        # 야후 quoteSummary가 막혀 PER/PBR/배당수익률이 비어있으면 pykrx(KRX 원천)로 보강
        if result.get('per') is None or result.get('pbr') is None:
            fallback_fund = fetch_kr_fundamentals_pykrx(code)
            for k, v in fallback_fund.items():
                if result.get(k) is None and v is not None:
                    result[k] = v

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
    # light=1이면 이름 조회(ticker.info)와 PER/PBR/재무제표 조회를 모두 건너뛰고
    # 가격/등락률/차트만 빠르게 반환한다. 지수 위젯처럼 이름·재무지표가 필요 없는
    # 호출에 사용해 야후 API 호출 횟수를 줄이기 위함.
    light = request.args.get('light', '').strip().lower() in ('1', 'true', 'yes')

    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    cache_key = f"G:{symbol}:{period}:{'light' if light else 'full'}"
    cached = get_cached_stock(cache_key)
    if cached:
        return jsonify(cached)

    try:
        quote = fetch_yahoo_quote(symbol, period, light=light)
        ticker = quote.pop('_ticker')

        result = {
            'symbol': symbol,
            'name': None if light else get_foreign_name(symbol, ticker),
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
    result = _yahoo_call_with_retry(yf.screen, q, sortField='intradaymarketcap', sortAsc=False, offset=offset, size=count)
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
        to_resolve = []  # (item, bare_code) — 벌크 캐시에 없어 개별 보강이 필요한 항목
        for it in deduped:
            # 야후가 앞자리 0을 뺀 코드(예: '5930')를 내려주는 경우가 있어
            # KRX 캐시 키(6자리 zero-pad)와 어긋나 한글명을 못 찾는 문제를 방지.
            bare_code = (it['symbol'] or '').split('.')[0].zfill(6)
            korean_name = krx_names.get(bare_code)
            if korean_name:
                it['name'] = korean_name
            else:
                to_resolve.append((it, bare_code))

        # 벌크 소스(FDR/KIND)가 상당수 종목을 놓치는 경우, 개별 보강 조회(pykrx)를
        # 순차 실행하면 종목 수만큼 네트워크 왕복이 누적돼 응답이 매우 느려진다.
        # 이를 스레드풀로 병렬 실행해 전체 대기시간을 크게 줄인다.
        unresolved = []
        if to_resolve:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(20, len(to_resolve))) as executor:
                future_to_item = {
                    executor.submit(resolve_missing_kr_name, bare_code): (it, bare_code)
                    for it, bare_code in to_resolve
                }
                for future in concurrent.futures.as_completed(future_to_item):
                    it, bare_code = future_to_item[future]
                    try:
                        korean_name = future.result()
                    except Exception as e:
                        print(f"[개별 종목명 보강 실패] {bare_code}: {e}")
                        korean_name = None
                    if korean_name:
                        it['name'] = korean_name
                    else:
                        unresolved.append(bare_code)
        if unresolved:
            print(f"[KR 스크리너] 한글명 미해결 코드 {len(unresolved)}개: {unresolved}")

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
