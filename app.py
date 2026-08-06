import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
from curl_cffi import requests as requests_cffi

app = Flask(__name__)
CORS(app)

# 차단 우회를 위한 브라우저 세션 생성 (Chrome 브라우저 위장)
session = requests_cffi.Session(impersonate="chrome110")

# 종목명은 자주 안 바뀌므로 서버 메모리에 캐시해서 야후 요청 횟수를 줄임
_name_cache = {}

def get_stock_name(ticker, symbol):
    if symbol in _name_cache:
        return _name_cache[symbol]
    name = symbol
    try:
        info = ticker.info  # longName/shortName은 fast_info엔 없어서 별도 조회 필요
        name = info.get('longName') or info.get('shortName') or symbol
    except Exception:
        pass
    _name_cache[symbol] = name
    return name

@app.route('/api/stock')
def get_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    
    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    if symbol.isdigit() and len(symbol) == 6:
        symbol += ".KS"

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

        name = get_stock_name(ticker, symbol)

        return jsonify({
            'symbol': symbol,
            'name': name,
            'currentPrice': current_price,
            'previousClose': previous_close,
            'change': change,
            'changePercent': change_percent,
            'currency': fast_info.currency or 'USD',
            'high52': fast_info.year_high,
            'low52': fast_info.year_low,
            'marketCap': fast_info.market_cap,
            'chart': chart_data
        })

    except Exception as e:
        return jsonify({'error': f'요청 제한 또는 오류 발생: ({str(e)})'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
