import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
CORS(app)

@app.route('/api/stock')
def get_stock():
    symbol = request.args.get('symbol', '').strip().upper()
    
    if not symbol:
        return jsonify({'error': '종목 코드를 입력해주세요.'}), 400

    if symbol.isdigit() and len(symbol) == 6:
        symbol += ".KS"

    try:
        ticker = yf.Ticker(symbol)
        
        info_all = ticker.info
        fast_info = ticker.fast_info
        
        current_price = fast_info.last_price or info_all.get('currentPrice')
        previous_close = fast_info.previous_close or info_all.get('previousClose')
        
        if current_price is None:
            raise ValueError("데이터 없음")

        change = current_price - previous_close if previous_close else 0
        change_percent = (change / previous_close) * 100 if previous_close else 0

        # 🔥 1. 최근 30일 차트 데이터 가져오기 (날짜 & 종가)
        history = ticker.history(period="1mo")
        chart_data = [
            {"date": date.strftime('%Y-%m-%d'), "close": round(row['Close'], 2)}
            for date, row in history.iterrows()
        ]

        # 🔥 2. 최신 뉴스 3건 가져오기
        raw_news = ticker.news or []
        news_list = []
        for item in raw_news[:3]:
            # yfinance 최신 버전 규격 대응
            content = item.get('content', item)
            news_list.append({
                'title': content.get('title'),
                'link': content.get('canonicalUrl') or content.get('link'),
                'publisher': content.get('provider', {}).get('displayName') if isinstance(content.get('provider'), dict) else ''
            })

        return jsonify({
            'symbol': symbol,
            'companyName': info_all.get('shortName') or info_all.get('longName') or symbol,
            'currentPrice': current_price,
            'previousClose': previous_close,
            'change': change,
            'changePercent': change_percent,
            'currency': fast_info.currency or info_all.get('currency', 'USD'),
            
            # 투자 핵심 지표
            'peRatio': info_all.get('trailingPE'),
            'priceToBook': info_all.get('priceToBook'),
            'dividendYield': info_all.get('dividendYield'),
            'high52': info_all.get('fiftyTwoWeekHigh'),
            'low52': info_all.get('fiftyTwoWeekLow'),
            'marketCap': info_all.get('marketCap'),
            
            # ✨ 신규 추가 데이터: 차트 & 뉴스
            'chart': chart_data,
            'news': news_list,
            'raw_info': info_all
        })

    except Exception as e:
        return jsonify({'error': f'주식 정보를 불러오지 못했습니다. ({str(e)})'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
