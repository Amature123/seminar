# from vnstock import Trading
# import pandas as pd
# pd.set_option('display.max_columns', None)
# trading = Trading(symbol='VN30F1M',source='vci')
# board = trading.price_board(["ACB"])
# print(board.info())
# # from vnstock import Quote
# # quote = Quote(symbol='ACB', source='vci')
# # OHVCL = quote.history(start="2025-12-10", end='2025-12-10', interval='1m')
# # print(OHVCL.iloc[-1])
# from vnstock import Quote
# from datetime import datetime,timedelta,time
# from zoneinfo import ZoneInfo
# from vnstock import Trading
# from vnstock import Quote
# import pandas as pd
# import urllib.request
# import json
# pd.set_option('display.max_rows', None)
# import json
# from zoneinfo import ZoneInfo
# vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")
# today = datetime.now(vietnamese_timezone)
import requests
import json
def my_custom_function():
    url = "https://api.worldnewsapi.com/search-news?text=thi+truong+chung+khoan+ACB&number=100&earliest-publish-date=2026-01-01&source-country=vn"
    api_key = "dcecd661ab884632936c9e49047eebba"
    headers = {
        'x-api-key': api_key
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"

if __name__ == "__main__":
    data = my_custom_function()

    if data:
        with open("vietnam_stock_news.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print("✅ Đã lưu file vietnam_stock_news.json")
    else:
        print("❌ Lỗi khi gọi API")