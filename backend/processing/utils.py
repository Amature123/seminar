from vnstock import Listing
import json
from datetime import datetime
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = cp_list
<<<<<<< HEAD:script/processing/utils.py

=======
>>>>>>> 1eca910 (add backend):backend/processing/utils.py
def safe_json_deserializer(x):
    if x is None:
        return None
    return json.loads(x.decode("utf-8"))

def transform_time(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)