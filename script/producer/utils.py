from vnstock import Listing
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = cp_list