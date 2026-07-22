import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
# 모바일, PC 등 모든 브라우저 접근 허용 (CORS 완전 해제)
CORS(app)

@app.route('/api/stock')
def get_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    
    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    # 6자리 숫자인 경우 코스피(.KS) 기본 처리
    if symbol.isdigit() and len(symbol) == 6:
        symbol += ".KS"

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        
        current_price = info.last_price
        previous_close = info.previous_close
        
        if current_price is None or previous_close is None:
            raise ValueError("데이터 없음")

        change = current_price - previous_close
        change_percent = (change / previous_close) * 100
        currency = info.currency

        return jsonify({
            'symbol': symbol,
            'currentPrice': current_price,
            'previousClose': previous_close,
            'change': change,
            'changePercent': change_percent,
            'currency': currency
        })

    except Exception:
        return jsonify({'error': '주식 정보를 불러오지 못했습니다. 종목 코드를 확인해주세요.'}), 404

if __name__ == '__main__':
    # 클라우드 서버(Render 등) 환경 대응을 위한 포트 설정
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)