import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
from curl_cffi import requests as requests_cffi

app = Flask(__name__)
CORS(app)

# 차단 우회를 위한 브라우저 세션 생성 (Chrome 브라우저 위장)
session = requests_cffi.Session(impersonate="chrome110")

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

        return jsonify({
            'symbol': symbol,
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
