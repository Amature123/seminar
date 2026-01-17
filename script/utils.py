from vnstock import Listing, Company
import pandas as pd
import re
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = cp_list

def extract_company_info(symbol)->pd.DataFrame:
    company = Company(symbol=symbol,source ='vci')
    return company.overview()

def extract_history(symbol):
    text = extract_company_info(symbol)['history'].iloc[0]
    pattern = r"-\s*(Ngày\s\d{2}/\d{2}/\d{4}|Năm\s\d{4}):\s*(.*?);"
    matches = re.findall(pattern, text)
    df = pd.DataFrame(matches, columns=["date", "detail"])
    return df

def extract_company_profile(symbol):
    return extract_company_info(symbol)['company_profile'].iloc[0]
