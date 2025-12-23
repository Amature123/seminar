import json
import time
import uuid
import logging
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Trading
from utils import SYMBOLS
from zoneinfo import ZoneInfo


vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOPIC = 'trading_data'
BOOTSTRAP_SERVERS = ['kafka_broker:19092']


def json_serializer(data):
    if isinstance(data, (np.integer, np.floating)):
        return data.item()
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")


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



def extract_trading_data(symbol):
    handle_time = datetime.now(vietnamese_timezone)

    try:
        trading = Trading(symbol='VN30F1M', source='vci')
        board = trading.price_board([symbol.upper()])

        logger.info(
            f"Fetched data for {symbol}: "
            f"{board.listing.sending_time[0] if board.listing is not None else None}"
        )

        record = {
            "handle_time": handle_time.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "ceiling": board.listing.ceiling[0] if board.listing is not None else None,
            "floor": board.listing.floor[0] if board.listing is not None else None,
            "reference": board.listing.ref_price[0] if board.listing is not None else None,
            "Room_foreign": board.match.current_room[0] if board.match is not None else None,
            "foreign_buy_volume": board.match.foreign_buy_volume[0] if board.match is not None else None,
            "foreign_sell_volume": board.match.foreign_sell_volume[0] if board.match is not None else None,
            "Buy": {
                "Buy_1": board.bid_ask.bid_1_price[0] if board.bid_ask is not None else None,
                "Buy_1_volume": board.bid_ask.bid_1_volume[0] if board.bid_ask is not None else None,
                "Buy_2": board.bid_ask.bid_2_price[0] if board.bid_ask is not None else None,
                "Buy_2_volume": board.bid_ask.bid_2_volume[0] if board.bid_ask is not None else None,
                "Buy_3": board.bid_ask.bid_3_price[0] if board.bid_ask is not None else None,
                "Buy_3_volume": board.bid_ask.bid_3_volume[0] if board.bid_ask is not None else None,
            },
            "Sell": {
                "Sell_1": board.bid_ask.ask_1_price[0] if board.bid_ask is not None else None,
                "Sell_1_volume": board.bid_ask.ask_1_volume[0] if board.bid_ask is not None else None,
                "Sell_2": board.bid_ask.ask_2_price[0] if board.bid_ask is not None else None,
                "Sell_2_volume": board.bid_ask.ask_2_volume[0] if board.bid_ask is not None else None,
                "Sell_3": board.bid_ask.ask_3_price[0] if board.bid_ask is not None else None,
                "Sell_3_volume": board.bid_ask.ask_3_volume[0] if board.bid_ask is not None else None,
            },
            "highest": board.match.highest[0] if board.match is not None else None,
            "lowest": board.match.lowest[0] if board.match is not None else None,
            "average": (
                (board.match.highest[0] + board.match.lowest[0]) / 2
                if board.match is not None else None
            ),
        }

        record = {
            k: (v.item() if isinstance(v, np.generic) else v)
            for k, v in record.items()
        }

        logger.info(f"Record built for {symbol}: {record}")
        return record

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None


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


def run_producer(symbols, sleep_time=60):
    producer = create_producer()
    logger.info(f"Producing trading data for symbols: {symbols}")

    while True:
        try:
            for symbol in symbols:
                record = extract_trading_data(symbol)
                produce_message(producer, record)
                logger.info(f"Record to be sent: {record}")

            producer.flush()
            logger.info("All messages sent.")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
            break
        except Exception as e:
            logger.error(f"Producer loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    logger.info("Starting Kafka Trading producer")
    run_producer(SYMBOLS, sleep_time=60)
