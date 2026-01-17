from vnstock import Listing, Company
import pandas as pd
import re
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = cp_list
