import json
from datetime import datetime
# Danh sách VN30 (hardcode; đã bỏ vnstock)
SYMBOLS = [
    'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG',
    'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB',
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE',
]
def safe_json_deserializer(x):
    if x is None:
        return None
    return json.loads(x.decode("utf-8"))

def transform_time(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)