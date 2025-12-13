import json
from math import log
import time
import uuid
import logging
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Quote,Trading
import time
from utils import symbol_list
from zoneinfo import ZoneInfo

vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class KafkaTradingProducer:
    def __init__(self, topic='stock_data', bootstrap_servers='localhost:9092'):
        self.topic = topic
        
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8')
        )
        try : 
            Trading(symbol='VN30F1M',source='vci')
            self.source = 'vci'
        except Exception:
            self.source = 'tcbs'
        self.checkpoint = {}

    def json_serializer(self, data):
        if isinstance(data, (np.integer, np.floating)):
            return data.item()
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def delivery_report(self, record_metadata):
        logger.info(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}")

    def extract_stock_data(self, symbol):
        handle_time = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        try:
            trading = Trading(symbol='VN30F1M',source='vci')
            board = trading.price_board([symbol.upper()])
            logger.info(f"Fetched data for {symbol}: {board.listing.sending_time[0]}")
            record = {
                "handle_time":handle_time.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "ceiling": board.listing.ceiling[0] if board.listing is not None else None,
                "floor": board.listing.floor[0] if board.listing is not None else None,
                "reference": board.listing.ref_price[0] if board.listing is not None else None,
                "Room_foreign": board.match.current_room[0] if board.match is not None else None,
                "foreign_buy_volume": board.match.foreign_buy_volume[0] if board.match is not None else None,
                "foreign_sell_volume": board.match.foreign_sell_volume[0] if board.match is not None else None,
                "Buy":{
                    "Buy_1": board.bid_ask.bid_1_price[0] if board.bid_ask is not None else None,
                    "Buy_1_volume": board.bid_ask.bid_1_volume[0] if board.bid_ask is not None else None,
                    "Buy_2": board.bid_ask.bid_2_price[0] if board.bid_ask is not None else None,
                    "Buy_2_volume": board.bid_ask.bid_2_volume[0] if board.bid_ask is not None else None,
                    "Buy_3": board.bid_ask.bid_3_price[0] if board.bid_ask is not None else None,
                    "Buy_3_volume": board.bid_ask.bid_3_volume[0] if board.bid_ask is not None else None,
                },
                "Sell":{
                    "Sell_1": board.bid_ask.ask_1_price[0] if board.bid_ask is not None else None,
                    "Sell_1_volume": board.bid_ask.ask_1_volume[0] if board.bid_ask is not None else None,
                    "Sell_2": board.bid_ask.ask_2_price[0] if board.bid_ask is not None else None,
                    "Sell_2_volume": board.bid_ask.ask_2_volume[0] if board.bid_ask is not None else None,
                    "Sell_3": board.bid_ask.ask_3_price[0] if board.bid_ask is not None else None,
                    "Sell_3_volume": board.bid_ask.ask_3_volume[0] if board.bid_ask is not None else None,
                },
                "highest" : board.match.highest[0] if board.match is not None else None,
                "lowest" :board.match.lowest[0] if board.match is not None else None,
                "average": (board.match.highest[0] + board.match.lowest[0])/2 if board.match is not None else None,
            }
            logger.info(f"Fetched data for {symbol}: {record}")
            record = {k: (v.item() if isinstance(v, np.generic) else v) for k, v in record.items()}
            return record
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None

    def produce_messages(self, record):
        try:

            future = self.producer.send(self.topic, value=record)
            future.add_callback(self.delivery_report)
            future.add_errback(lambda e: logger.error(f"Message delivery failed: {e}"))
            
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    def run(self, symbols, sleep_time=10):
        logger.info(f"Producing messages for symbols: {symbols}")
        
        try:
            for symbol in symbols:
                try:
                    record = self.extract_stock_data(symbol)
                    self.produce_messages(record)
                    logger.info(f"Record to be sent: {record}")
                except Exception as e:
                    logger.error(f"Error in producing loop for {symbol}: {e}")
            self.producer.flush()
            logger.info("All messages sent.")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Error at procedure: {e}")

if __name__ == "__main__":
    producer = KafkaTradingProducer(
        topic='trading_data',
        bootstrap_servers='kafka_broker:19092'
    )
    
    symbols = symbol_list
    logger.info("Starting Kafka producer for symbols: %s", symbols)
    while True:
        try:
            producer.run(symbols, sleep_time=60) 
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
