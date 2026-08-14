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


# ---------------------------------------------------------
# 야후 quoteSummary(PER/PBR/ROE 등)는 chart API(가격/차트)와 달리 crumb(인증 토큰) +
# 쿠키 핸드셰이크를 요구한다. yfinance의 ticker.info가 이 핸드셰이크에 실패하는 경우가
# (특히 Render 등 클라우드 IP에서) 흔해서, crumb를 직접 발급받아 quoteSummary를
# 수동으로 호출하는 최후 폴백을 둔다.
# ---------------------------------------------------------
_yahoo_crumb_cache = {'crumb': None, 'ts': 0.0}
_YAHOO_CRUMB_TTL = 60 * 60  # crumb는 자주 안 바뀌므로 1시간 캐시


def _get_yahoo_crumb():
    now = time.time()
    if _yahoo_crumb_cache['crumb'] and (now - _yahoo_crumb_cache['ts']) < _YAHOO_CRUMB_TTL:
        return _yahoo_crumb_cache['crumb']

    try:
        # 쿠키를 먼저 확보해야 crumb 발급이 성공할 확률이 높아짐
        _yahoo_call_with_retry(session.get, 'https://fc.yahoo.com', timeout=6, retries=1)
    except Exception as e:
        print(f"[야후 쿠키 확보 실패] {e}")

    try:
        resp = _yahoo_call_with_retry(
            session.get, 'https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=6, retries=1
        )
        crumb = (resp.text or '').strip()
        if crumb and len(crumb) < 50 and 'error' not in crumb.lower() and '<html' not in crumb.lower():
            _yahoo_crumb_cache['crumb'] = crumb
            _yahoo_crumb_cache['ts'] = now
            return crumb
        print(f"[야후 crumb 응답 이상] {crumb[:100]!r}")
    except Exception as e:
        print(f"[야후 crumb 발급 실패] {e}")
    return None


def fetch_fundamentals_via_quotesummary(symbol: str) -> dict:
    """crumb 인증과 함께 quoteSummary API를 직접 호출해 PER/PBR/ROE/부채비율/배당수익률을 조회.
    ticker.info가 실패했을 때만 호출되는 최후 폴백."""
    crumb = _get_yahoo_crumb()
    if not crumb:
        return {}

    def _raw(v):
        return v.get('raw') if isinstance(v, dict) else v

    try:
        url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}'
        params = {
            'modules': 'defaultKeyStatistics,financialData,summaryDetail',
            'crumb': crumb,
        }
        resp = _yahoo_call_with_retry(session.get, url, params=params, timeout=8)
        data = resp.json()
        result = (data.get('quoteSummary') or {}).get('result') or []
        if not result:
            print(f"[quoteSummary 직접 호출] {symbol}: 결과 없음 - {str(data)[:200]}")
            return {}

        modules = result[0]
        key_stats = modules.get('defaultKeyStatistics', {}) or {}
        fin_data = modules.get('financialData', {}) or {}
        summary = modules.get('summaryDetail', {}) or {}

        per = _raw(summary.get('trailingPE')) or _raw(key_stats.get('forwardPE'))
        pbr = _raw(key_stats.get('priceToBook'))
        roe = _raw(fin_data.get('returnOnEquity'))
        debt = _raw(fin_data.get('debtToEquity'))
        raw_div_yield = _raw(summary.get('dividendYield'))

        if raw_div_yield is None or (isinstance(raw_div_yield, float) and raw_div_yield != raw_div_yield):
            dividend_yield = None
        elif float(raw_div_yield) > 1.5:
            dividend_yield = round(float(raw_div_yield), 2)
        else:
            dividend_yield = _pct(raw_div_yield)

        print(f"[quoteSummary 직접 호출 성공] {symbol}: PER={per} PBR={pbr} ROE={roe}")
        return {
            'per': None if _is_nan(per) else round(float(per), 2),
            'pbr': None if _is_nan(pbr) else round(float(pbr), 2),
            'roe': _pct(roe),
            'debtRatio': None if _is_nan(debt) else round(float(debt), 1),
            'dividendYield': dividend_yield,
        }
    except Exception as e:
        print(f"[quoteSummary 직접 호출 실패] {symbol}: {e}")
        return {}

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
    예전에는 pykrx로 보강했으나, pykrx가 최근 KRX 로그인(KRX_ID/KRX_PW)을
    요구하도록 바뀌면서 매번 실패할 뿐 아니라, 락 대기 중 요청이 무한정
    멈춰 서버가 타임아웃으로 죽는 문제까지 일으켜 완전히 비활성화한다.
    벌크 캐시에 없는 종목은 이후 get_foreign_name(야후 이름)으로 폴백된다."""
    return _krx_name_cache.get(code)


def fetch_kr_fundamentals_pykrx(code: str) -> dict:
    """예전에는 pykrx로 PER/PBR/배당수익률을 보강했으나, pykrx가 최근 KRX
    로그인(KRX_ID/KRX_PW)을 요구하도록 바뀌면서 항상 실패하고, 경우에 따라
    응답이 멈춰 서버가 타임아웃으로 죽는 문제까지 있어 완전히 비활성화한다.
    같은 역할은 아래 fetch_kr_fundamentals_dart()가 대체한다."""
    return {}


# ---------------------------------------------------------
# DART(전자공시) OpenAPI 무료 폴백 - 국내 종목 PER/PBR/ROE
# 야후 quoteSummary(crumb)와 pykrx가 모두 클라우드 IP 차단으로 막힐 때 쓰는
# 최종 폴백. DART는 정부 공식 REST API라 IP 차단이 없다.
# 무료 키 발급: https://opendart.fss.or.kr (이메일 인증 후 즉시 발급, 환경변수 DART_API_KEY에 설정)
# 주당 지표(EPS/BPS) 대신 회사 전체 지표(시가총액/순이익, 시가총액/자본총계)를
# 사용해서 계산 -> 발행주식수를 따로 조회할 필요가 없어 구현이 단순해짐.
# ---------------------------------------------------------
DART_API_KEY = os.environ.get('DART_API_KEY', '')

_dart_corp_code_cache = {}  # {'005930': '00126380', ...} 종목코드 -> DART 고유번호
_dart_corp_code_lock = threading.Lock()
_dart_corp_code_updated_at = 0
_DART_CORP_CODE_TTL = 24 * 60 * 60  # 24시간


def _load_dart_corp_codes() -> dict:
    """DART가 제공하는 전체 상장사 corp_code 매핑표(zip 안의 XML)를 내려받아 캐싱."""
    global _dart_corp_code_cache, _dart_corp_code_updated_at
    if not DART_API_KEY:
        return {}

    now = time.time()
    if _dart_corp_code_cache and (now - _dart_corp_code_updated_at) < _DART_CORP_CODE_TTL:
        return _dart_corp_code_cache

    with _dart_corp_code_lock:
        now = time.time()
        if _dart_corp_code_cache and (now - _dart_corp_code_updated_at) < _DART_CORP_CODE_TTL:
            return _dart_corp_code_cache

        try:
            import zipfile
            import io
            import xml.etree.ElementTree as ET

            # 브라우저 위장 없는 맨 요청(requests_cffi.get)은 DART 쪽에서 순간적으로
            # 끊기는 경우가 있어(2~3초 만에 connection timeout), 이미 야후 호출에
            # 쓰던 크롬 위장 세션(session)을 재사용한다. 실패 시 1회 재시도.
            resp = None
            last_err = None
            for attempt in range(2):
                try:
                    resp = session.get(
                        'https://opendart.fss.or.kr/api/corpCode.xml',
                        params={'crtfc_key': DART_API_KEY},
                        timeout=30,
                    )
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(1.5)
            if resp is None:
                raise last_err

            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            xml_bytes = zf.read('CORPCODE.xml')
            root = ET.fromstring(xml_bytes)

            new_map = {}
            for item in root.iter('list'):
                stock_code = (item.findtext('stock_code') or '').strip()
                corp_code = (item.findtext('corp_code') or '').strip()
                if stock_code and corp_code:
                    new_map[stock_code.zfill(6)] = corp_code

            if new_map:
                _dart_corp_code_cache = new_map
                _dart_corp_code_updated_at = now
                print(f"[DART corp_code 캐시] {len(new_map)}개 종목 매핑 완료")
        except Exception as e:
            print(f"[DART corp_code 캐시 갱신 실패] {e}")

        return _dart_corp_code_cache


def fetch_kr_fundamentals_dart(code: str, market_cap) -> dict:
    """DART 재무제표(당기순이익/자본총계)로 PER/PBR/ROE를 계산.
    pykrx/야후가 모두 막혔을 때의 최종 폴백."""
    if not DART_API_KEY or not market_cap:
        return {}

    corp_map = _load_dart_corp_codes()
    corp_code = corp_map.get(code)
    if not corp_code:
        return {}

    from datetime import datetime
    this_year = datetime.now().year

    # 최신 사업보고서(11011)부터 역순으로 최대 2개년 탐색
    # (연초에는 아직 전년도 사업보고서가 안 올라온 경우가 있어서)
    for year in (this_year - 1, this_year - 2):
        for fs_div in ('CFS', 'OFS'):  # 연결재무제표 우선, 없으면 개별재무제표
            try:
                resp = session.get(
                    'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json',
                    params={
                        'crtfc_key': DART_API_KEY,
                        'corp_code': corp_code,
                        'bsns_year': str(year),
                        'reprt_code': '11011',
                        'fs_div': fs_div,
                    },
                    timeout=15,
                )
                data = resp.json()
                rows = data.get('list') or []
                if not rows:
                    continue

                net_income = None
                total_equity = None
                for row in rows:
                    name = (row.get('account_nm') or '').strip()
                    amt = row.get('thstrm_amount')
                    if amt in (None, ''):
                        continue
                    try:
                        amt_val = float(str(amt).replace(',', ''))
                    except ValueError:
                        continue
                    if name == '당기순이익' and net_income is None:
                        net_income = amt_val
                    elif name == '자본총계' and total_equity is None:
                        total_equity = amt_val

                if net_income is None and total_equity is None:
                    continue

                result = {}
                if total_equity:
                    result['pbr'] = round(market_cap / total_equity, 2)
                    if net_income is not None:
                        result['roe'] = round((net_income / total_equity) * 100, 2)
                if net_income:
                    result['per'] = round(market_cap / net_income, 2)

                if result:
                    print(f"[DART 재무지표 보강 성공] {code} ({year}년/{fs_div}): {result}")
                    return result
            except Exception as e:
                print(f"[DART 재무지표 조회 실패] {code} ({year}년/{fs_div}): {e}")
                continue

    return {}


# ---------------------------------------------------------
# Finnhub 무료 API 폴백 - 해외 종목 PER/PBR/ROE
# 야후 quoteSummary가 클라우드 IP 차단으로 막혔을 때의 해외 종목용 폴백.
# 무료 키 발급: https://finnhub.io (가입 즉시 발급, 분당 60회 제한, 환경변수 FINNHUB_API_KEY에 설정)
# ---------------------------------------------------------
FINNHUB_API_KEY = os.environ.get('FINNHUB_API_KEY', '')


def fetch_us_fundamentals_finnhub(symbol: str) -> dict:
    if not FINNHUB_API_KEY:
        return {}
    try:
        resp = requests_cffi.get(
            'https://finnhub.io/api/v1/stock/metric',
            params={'symbol': symbol, 'metric': 'all', 'token': FINNHUB_API_KEY},
            timeout=8,
        )
        data = resp.json()
        metric = data.get('metric') or {}
        if not metric:
            return {}

        per = metric.get('peTTM') or metric.get('peBasicExclExtraTTM')
        pbr = metric.get('pb')
        roe = metric.get('roeTTM')

        result = {
            'per': None if _is_nan(per) else round(float(per), 2),
            'pbr': None if _is_nan(pbr) else round(float(pbr), 2),
            'roe': None if _is_nan(roe) else round(float(roe), 2),
        }
        result = {k: v for k, v in result.items() if v is not None}
        if result:
            print(f"[Finnhub 재무지표 보강 성공] {symbol}: {result}")
        return result
    except Exception as e:
        print(f"[Finnhub 재무지표 조회 실패] {symbol}: {e}")
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
    ticker.info가 쓰는 야후의 quoteSummary API는 가격/차트에 쓰이는 chart API와 달리
    crumb(인증 토큰) 핸드셰이크가 필요해서, 클라우드 서버 IP(Render 등)에서는
    거의 항상 'Invalid Crumb'로 거부된다. 재시도/수동 crumb 폴백 모두 결국 실패하는데
    시간만 잡아먹어(요청당 수십 초 -> 게이트웨이 타임아웃 원인) retries=0으로 1회만
    시도하고, 실패하면 바로 포기해서 pykrx/DART/Finnhub 같은 살아있는 폴백에
    빨리 넘어가도록 한다."""
    try:
        info = _yahoo_call_with_retry(lambda: ticker.info or {}, retries=0)
    except Exception as e:
        print(f"[재무지표 조회 실패 - ticker.info] {ticker.ticker}: {e}")
        info = {}

    if not info:
        # 야후 crumb 수동 폴백은 클라우드 IP에서 구조적으로 막혀 있어(Invalid Crumb)
        # 더 이상 시도하지 않는다. pykrx/DART/Finnhub 폴백이 호출부에서 이어서 처리한다.
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

    def _read_history():
        return _yahoo_call_with_retry(ticker.history, period=_PERIOD_MAP.get(period, '1mo'))

    # fast_info/history/fundamentals/financials는 서로 독립적인 호출이라
    # 순차 실행 대신 스레드로 동시에 시작한다. 실제 야후 전송은 여전히
    # _yahoo_call_with_retry의 전역 스로틀(0.4초 간격)로 줄을 서지만,
    # 각 호출의 "응답 대기" 구간이 서로 겹치게 되어 종목 하나당 걸리는
    # 총 시간이 (순차 합산) 대신 (가장 느린 호출 기준)에 가까워진다.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        fast_info_fut = executor.submit(_yahoo_call_with_retry, _read_fast_info)
        history_fut = executor.submit(_read_history)
        if light:
            fundamentals_fut = None
            financials_fut = None
        else:
            fundamentals_fut = executor.submit(fetch_fundamentals, ticker)
            financials_fut = executor.submit(fetch_annual_financials, ticker)

        current_price, previous_close, currency, year_high, year_low, market_cap = fast_info_fut.result()
        history = history_fut.result()
        fundamentals = fundamentals_fut.result() if fundamentals_fut else {
            'per': None, 'pbr': None, 'roe': None, 'debtRatio': None, 'dividendYield': None
        }
        financials = financials_fut.result() if financials_fut else []

    if current_price is None:
        raise ValueError("데이터 없음")

    change = current_price - previous_close if previous_close else 0
    change_percent = (change / previous_close) * 100 if previous_close else 0

    chart_data = [
        {"date": date.strftime('%Y-%m-%d'), "close": round(row['Close'], 2)}
        for date, row in history.iterrows()
    ]

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
    # light=1이면 PER/PBR/재무제표(및 그 보강용 pykrx/DART 폴백 체인)를 모두 건너뛰고
    # 가격/등락률/차트만 빠르게 반환한다. 즐겨찾기 목록처럼 이름/가격/스파크라인만
    # 필요한 화면에 사용해 야후·DART 호출 횟수와 응답 시간을 크게 줄이기 위함.
    light = request.args.get('light', '').strip().lower() in ('1', 'true', 'yes')

    if not is_kr_code(code):
        return jsonify({'error': '국내 종목 코드는 6자리 코드여야 합니다.'}), 400

    cache_key = f"KR:{code}:{period}:{'light' if light else 'full'}"
    cached = get_cached_stock(cache_key)
    if cached:
        return jsonify(cached)

    yahoo_symbol = code + get_yahoo_suffix(code)

    try:
        quote = fetch_yahoo_quote(yahoo_symbol, period, light=light)
        ticker = quote.pop('_ticker')

        # 1순위: KRX 종목명 벌크 캐시. 2순위: 개별 보강 조회(pykrx).
        # 그래도 없으면(신규 상장 등) 야후 이름으로 최종 폴백.
        name = get_krx_name_map().get(code) or resolve_missing_kr_name(code) or get_foreign_name(yahoo_symbol, ticker)

        result = {
            'symbol': code,
            'name': name,
            **quote,
        }

        if not light:
            # 야후 quoteSummary가 막혀 PER/PBR/배당수익률이 비어있으면 pykrx(KRX 원천)로 보강
            if result.get('per') is None or result.get('pbr') is None:
                fallback_fund = fetch_kr_fundamentals_pykrx(code)
                for k, v in fallback_fund.items():
                    if result.get(k) is None and v is not None:
                        result[k] = v

            # pykrx도 막혀있으면 DART(정부 공식 API, IP 차단 없음)로 최종 보강
            if result.get('per') is None or result.get('pbr') is None or result.get('roe') is None:
                dart_fund = fetch_kr_fundamentals_dart(code, result.get('marketCap'))
                for k, v in dart_fund.items():
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

        # 야후 quoteSummary가 막혀 PER/PBR/ROE가 비어있으면 Finnhub(정식 API)로 보강
        if not light and (
            result.get('per') is None or result.get('pbr') is None or result.get('roe') is None
        ):
            finnhub_fund = fetch_us_fundamentals_finnhub(symbol)
            for k, v in finnhub_fund.items():
                if result.get(k) is None and v is not None:
                    result[k] = v

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
