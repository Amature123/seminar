import json
import time as t
import uuid
import schedule
import logging
import numpy as np
from datetime import datetime, time, timedelta
from kafka import KafkaProducer
from vnstock import Quote, Screener, Trading
from utils import SYMBOLS
from zoneinfo import ZoneInfo
from faker import Faker
import random
import numpy as np
vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")
today = datetime.now(vietnamese_timezone)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
LAST_SESSION_DATA = {}

TOPIC = 'ohvcl_data'
BOOTSTRAP_SERVERS = ['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092']
fake = Faker()


def json_serializer(data):
    if isinstance(data, (np.integer, np.floating)):
        return data.item()
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")

def check_trading_hour(data_time):
    if data_time.time()<datetime.time(9,30):
        last_day=data_time-datetime.timedelta(days=1)
        data_time=datetime.datetime(last_day.year,last_day.month,last_day.day,16,0,0)
        
    elif data_time.time()>datetime.time(16,0):
        data_time=datetime.datetime(data_time.year,data_time.month,data_time.day,16,0,0)
    return data_time

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(
            v, default=json_serializer
        ).encode('utf-8')
    )

def delivery_report(record_metadata):
    logger.info(
        f"Message delivered to {record_metadata.topic} "
        f"[{record_metadata.partition}] at offset {record_metadata.offset}"
    )
#----------------------Extract_data----------------------------#
def extract_stock_data(symbol):
    start_date =today.strftime("%Y-%m-%d")
    end_date =today.strftime("%Y-%m-%d")
    screen = '1m'
    try:
        quote = Quote(symbol=symbol, source='vci')
        ohvlc = quote.history(
            start=start_date,
            end=end_date,
            interval=screen
        )
        record = {
            "screener": screen,
            "symbol": symbol,
            "time": ohvlc["time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S"),
            "open": ohvlc["open"].iloc[-1],
            "high": ohvlc["high"].iloc[-1],
            "low": ohvlc["low"].iloc[-1],
            "close": ohvlc["close"].iloc[-1],
            "volume": ohvlc["volume"].iloc[-1]
        }
        return {
            k: (v.item() if isinstance(v, np.generic) else v)
            for k, v in record.items()
        }

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

def randomize_from_last_session(symbol, price_pct=0.005, vol_pct=0.2):
    global LAST_SESSION_DATA
    base = LAST_SESSION_DATA.get(symbol)
    if not base:
        return None
    close_price = base["close"]
    open_price = close_price
    close_price_new = close_price * random.uniform(1 - price_pct, 1 + price_pct) 
    high_price = max(open_price, close_price_new) * random.uniform(1.0, 1 + price_pct)
    low_price = min(open_price, close_price_new) * random.uniform(1 - price_pct, 1.0)
    volume = int(base["volume"] * random.uniform(1 - vol_pct, 1 + vol_pct))
    LAST_SESSION_DATA[symbol]['close'] = close_price_new

    return {
        "screener": "1m",
        "symbol": symbol,
        "time": datetime.now(vietnamese_timezone).strftime("%Y-%m-%d %H:%M:%S"),
        "open": round(open_price, 2),
        "high": round(high_price, 2),
        "low": round(low_price, 2),
        "close": round(close_price_new, 2),
        "volume": volume
    }


def load_last_session_data(symbols):
    global LAST_SESSION_DATA
    logger.info("Loading last session OHLC data...")
    for symbol in symbols:
        data = extract_stock_data(symbol)
        if data:
            LAST_SESSION_DATA[symbol] = data
            logger.info(f"Cached last session data for {symbol}")
        else:
            logger.warning(f"No data for {symbol}")
#----------------------Producer----------------------------#
def produce_message(producer, record):
    if not record:
        return
    try:
        future = producer.send(TOPIC, value=record)
        future.add_callback(delivery_report)
        future.add_errback(
            lambda e: logger.error(f"Message delivery failed: {e}")
        )
    except Exception as e:
        logger.error(f"Error producing message: {e}")


def kafka_producer_ohvlc(symbols,producer, realtime=True):
    try:  
        for symbol in symbols:
            logger.info(f"Producing messages for symbols: {symbol}")
            record = extract_stock_data(symbol) if realtime else randomize_from_last_session(symbol)
            produce_message(producer, record)
            logger.info(f"Record to be sent: {record}")
        producer.flush()
        logger.info("All messages sent.")
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Error in producer loop: {e}")
        t.sleep(5)


if __name__ == "__main__":
    logger.info("Starting Kafka OHVLC producer")
    producer = create_producer()
    #schedule
    if today.weekday()<5:
        if today.time() < time(9,0,0) or (today.time() > time(11,30,0) and today.time() < time(13,0,0)) or today.time() > time(14,30,0):
            load_last_session_data(SYMBOLS)
            kafka_producer_ohvlc(SYMBOLS,producer,False)
            schedule.every(2).seconds.do(kafka_producer_ohvlc,SYMBOLS,producer,False)
        else:
            kafka_producer_ohvlc(SYMBOLS,producer)
            schedule.every(60).seconds.do(kafka_producer_ohvlc,SYMBOLS,producer)
    else : 
        load_last_session_data(SYMBOLS)
        kafka_producer_ohvlc(SYMBOLS,producer,False)
        schedule.every(2).seconds.do(kafka_producer_ohvlc,SYMBOLS,producer,False)
    while True:
        schedule.run_pending()