import os
import time
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
from curl_cffi import requests as requests_cffi

app = Flask(__name__)
CORS(app)

# 차단 우회를 위한 브라우저 세션 생성 (Chrome 브라우저 위장)
session = requests_cffi.Session(impersonate="chrome110")

# ---------------------------------------------------------
# KRX(한국거래소) 종목코드 -> 종목명 캐시
# 네이버 등 개별 페이지를 매번 긁는 대신, KRX 전체 종목 리스트를
# 하루 한 번 정도만 갱신해서 메모리에 캐싱해두는 방식.
# 요청마다 외부 스크래핑을 하지 않으므로 차단될 일이 거의 없음.
# ---------------------------------------------------------
_krx_name_cache = {}
_krx_cache_lock = threading.Lock()
_krx_cache_updated_at = 0
_KRX_CACHE_TTL = 6 * 60 * 60  # 6시간마다 갱신


def get_krx_name_map():
    """{'005930': '삼성전자', ...} 형태의 딕셔너리를 반환 (캐시됨)"""
    global _krx_name_cache, _krx_cache_updated_at
    now = time.time()

    if _krx_name_cache and (now - _krx_cache_updated_at) < _KRX_CACHE_TTL:
        return _krx_name_cache

    with _krx_cache_lock:
        # 락 획득 대기 중 다른 스레드가 이미 갱신했을 수 있으니 재확인
        now = time.time()
        if _krx_name_cache and (now - _krx_cache_updated_at) < _KRX_CACHE_TTL:
            return _krx_name_cache

        try:
            import FinanceDataReader as fdr
            df = fdr.StockListing('KRX')  # 코스피+코스닥 전체 종목 리스트
            new_map = dict(zip(df['Code'].astype(str).str.zfill(6), df['Name']))
            if new_map:
                _krx_name_cache = new_map
                _krx_cache_updated_at = now
        except Exception as e:
            print(f"[KRX 종목명 캐시 갱신 실패] {e}")
            # 갱신 실패 시 기존 캐시(있다면) 유지

        return _krx_name_cache


def get_korean_name(symbol: str):
    """'005930' 또는 '005930.KS' 형태의 심볼에서 한글 종목명을 찾아 반환"""
    code = symbol.split('.')[0]
    if not (code.isdigit() and len(code) == 6):
        return None
    return get_krx_name_map().get(code)


def get_kr_stock_via_fdr(code: str):
    """
    국내 종목(6자리 코드)의 시세를 야후가 아니라 KRX 공식 데이터를 쓰는
    FinanceDataReader로 직접 조회. 야후 쪽 요청 제한(Too Many Requests)의
    영향을 받지 않아 국내 종목 조회가 훨씬 안정적이다.
    """
    import datetime
    import FinanceDataReader as fdr

    end = datetime.date.today()
    start = end - datetime.timedelta(days=370)  # 52주 고가/저가 계산용
    df = fdr.DataReader(code, start.isoformat(), end.isoformat())

    if df is None or df.empty:
        raise ValueError("데이터 없음")

    last_row = df.iloc[-1]
    prev_row = df.iloc[-2] if len(df) > 1 else last_row

    current_price = float(last_row['Close'])
    previous_close = float(prev_row['Close'])
    change = current_price - previous_close
    change_percent = (change / previous_close) * 100 if previous_close else 0

    recent = df.tail(30)  # 최근 1개월치 차트
    chart_data = [
        {"date": idx.strftime('%Y-%m-%d'), "close": round(float(row['Close']), 2)}
        for idx, row in recent.iterrows()
    ]

    return {
        'currentPrice': current_price,
        'previousClose': previous_close,
        'change': change,
        'changePercent': change_percent,
        'currency': 'KRW',
        'high52': float(df['High'].max()),
        'low52': float(df['Low'].min()),
        'marketCap': None,  # FDR 개별 종목 조회에는 시가총액이 없음
        'chart': chart_data
    }


# ---------------------------------------------------------
# 종목별 시세 조회 결과 캐시
# 같은 종목을 짧은 시간 안에 반복 조회할 때 야후 파이낸스에
# 매번 요청을 보내지 않고 캐시된 결과를 재사용해서,
# 'Too Many Requests' 요청 제한에 걸릴 확률을 줄인다.
# ---------------------------------------------------------
_stock_cache = {}
_stock_cache_lock = threading.Lock()
_STOCK_CACHE_TTL = 60  # 같은 종목은 60초 동안 캐시된 결과 재사용


def get_cached_stock(symbol: str):
    entry = _stock_cache.get(symbol)
    if entry and (time.time() - entry['ts']) < _STOCK_CACHE_TTL:
        return entry['data']
    return None


def set_cached_stock(symbol: str, data: dict):
    with _stock_cache_lock:
        _stock_cache[symbol] = {'data': data, 'ts': time.time()}

@app.route('/api/stock')
def get_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    
    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    if symbol.isdigit() and len(symbol) == 6:
        symbol += ".KS"

    # 캐시에 최근(60초 이내) 결과가 있으면 야후에 요청하지 않고 바로 반환
    cached = get_cached_stock(symbol)
    if cached:
        return jsonify(cached)

    code = symbol.split('.')[0]
    is_kr_stock = code.isdigit() and len(code) == 6
    korean_name = get_korean_name(symbol) if is_kr_stock else None

    # 국내 종목은 KRX 공식 데이터(FinanceDataReader)를 우선 사용.
    # 야후를 거치지 않으므로 'Too Many Requests' 문제 자체를 피한다.
    if is_kr_stock:
        try:
            price_data = get_kr_stock_via_fdr(code)
            result = {'symbol': symbol, 'name': korean_name, **price_data}
            set_cached_stock(symbol, result)
            return jsonify(result)
        except Exception as e:
            print(f"[FDR 조회 실패, 야후로 폴백] {symbol}: {e}")
            # 아래 야후 경로로 폴백

    try:
        # 우회 세션 적용
        ticker = yf.Ticker(symbol, session=session)
        fast_info = ticker.fast_info
        
        current_price = fast_info.last_price
        previous_close = fast_info.previous_close
        
        if current_price is None:
            raise ValueError("데이터 없음")

        change = current_price - previous_close if previous_close else 0
        change_percent = (change / previous_close) * 100 if previous_close else 0

        # 최근 1개월 차트 데이터
        history = ticker.history(period="1mo")
        chart_data = [
            {"date": date.strftime('%Y-%m-%d'), "close": round(row['Close'], 2)}
            for date, row in history.iterrows()
        ]

        result = {
            'symbol': symbol,
            'name': korean_name,  # 한글 종목명 (KRX 캐시에 없으면 null)
            'currentPrice': current_price,
            'previousClose': previous_close,
            'change': change,
            'changePercent': change_percent,
            'currency': fast_info.currency or 'USD',
            'high52': fast_info.year_high,
            'low52': fast_info.year_low,
            'marketCap': fast_info.market_cap,
            'chart': chart_data
        }

        set_cached_stock(symbol, result)

        return jsonify(result)

    except Exception as e:
        # 실패 시, 만료된 캐시라도 있으면 완전히 빈손으로 보내지 않고 재사용
        stale = _stock_cache.get(symbol)
        if stale:
            fallback = dict(stale['data'])
            fallback['stale'] = True  # 프론트에서 "약간 오래된 데이터" 표시에 활용 가능
            return jsonify(fallback)
        return jsonify({'error': f'요청 제한 또는 오류 발생: ({str(e)})'}), 404

# 서버 시작 시 백그라운드에서 KRX 종목명 캐시를 미리 예열
# (첫 요청 때 사용자가 몇 초씩 기다리지 않도록)
threading.Thread(target=get_krx_name_map, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
