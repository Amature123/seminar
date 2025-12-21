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
from vnstock import Quote
from datetime import datetime
from zoneinfo import ZoneInfo

vn_time = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
