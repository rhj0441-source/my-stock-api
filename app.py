import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import requests

app = Flask(__name__)
CORS(app)

# 야후 차단 방지를 위한 브라우저 헤더 설정
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

@app.route('/api/stock')
def get_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    
    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    if symbol.isdigit() and len(symbol) == 6:
        symbol += ".KS"

    try:
        ticker = yf.Ticker(symbol, session=session)
        
        # 차단율이 가장 적은 fast_info 활용
        fast_info = ticker.fast_info
        
        current_price = fast_info.last_price
        previous_close = fast_info.previous_close
        
        if current_price is None or previous_close is None:
            # fast_info 실패 시 일반 info 시도
            info = ticker.info
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            previous_close = info.get('previousClose')

        if current_price is None:
            raise ValueError("주식 데이터를 가져올 수 없습니다.")

        change = current_price - previous_close if previous_close else 0
        change_percent = (change / previous_close) * 100 if previous_close else 0

        # 최근 차트 데이터 (1개월)
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
