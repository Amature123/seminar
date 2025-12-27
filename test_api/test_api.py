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
from vnstock import Trading
from vnstock import Quote
import pandas as pd
pd.set_option('display.max_rows', None)

quote = Quote(symbol='BCM', source='VCI')
data = quote.history(start='2025-12-26', end='2025-12-26',interval = '5m')
print(data.tail(20))