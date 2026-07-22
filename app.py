from flask import Flask, request, jsonify
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
CORS(app)

@app.route('/api/stock', methods=['GET'])
def get_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    
    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    # 6자리 숫자만 입력 시 .KS 붙이기
    if symbol.isdigit() and len(symbol) == 6:
        symbol += '.KS'

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        
        current_price = info.get('lastPrice')
        previous_close = info.get('previousClose')
        
        if current_price is None or previous_close is None:
            return jsonify({'error': '주식 정보를 찾을 수 없습니다.'}), 404

        change = current_price - previous_close
        change_percent = (change / previous_close) * 100 if previous_close else 0

        return jsonify({
            'symbol': symbol,
            'currentPrice': current_price,
            'previousClose': previous_close,
            'change': change,
            'changePercent': change_percent,
            'currency': info.get('currency', 'KRW')
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
