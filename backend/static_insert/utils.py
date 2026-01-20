git add \
backend/core \
backend/processing/model_handler.py \
backend/processing/news_handler.py \
backend/processing/utils.py \
backend/producer/model_producer.py \
backend/run_batch.sh \
backend/static_insert/OHVLC_screener.py \
backend/static_insert/__init__.py \
backend/static_insert/ticks_news.py \
backend/static_insert/utils.py<<<<<<< HEAD:script/static_insert/utils.py
from vnstock import Listing, Company
import pandas as pd
import re
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = cp_list
=======
from vnstock import Listing
import logging
import uuid
company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = cp_list
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def json_serializer(data):
    if isinstance(data, (np.integer, np.floating)):
        return data.item()
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")

def delivery_report(record_metadata):
    logger.info(
        f"Message delivered to {record_metadata.topic} "
        f"[{record_metadata.partition}] at offset {record_metadata.offset}"
    )

>>>>>>> 1eca910 (add backend):backend/static_insert/utils.py
