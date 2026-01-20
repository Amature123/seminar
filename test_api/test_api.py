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
<<<<<<< HEAD
import requests
import json
def my_custom_function():
    url = "https://api.worldnewsapi.com/search-news?text=thi+truong+chung+khoan+ACB&number=100&earliest-publish-date=2026-01-01&source-country=vn"
    api_key = "dcecd661ab884632936c9e49047eebba"
    headers = {
        'x-api-key': api_key
    }
    response = requests.get(url, headers=headers)
=======
from vnstock import Listing, Company
import pandas as pd
import re
import random
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS =random.sample(list(cp_list), 18)
>>>>>>> 1eca910 (add backend)

print(SYMBOLS)