import json
import time as t
import uuid
import logging
import random
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from utils import SYMBOLS
from zoneinfo import ZoneInfo
import schedule


vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOPIC = 'ticks_symbol'
BOOTSTRAP_SERVERS = ['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092']
######-------handle--------#########
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
###-------data----------###########
NEWS_TEMPLATES = [
    ("{s}: Khối ngoại mua ròng phiên hôm nay", "Nhà đầu tư nước ngoài tiếp tục gom {s}."),
    ("{s} công bố kết quả kinh doanh quý", "Lợi nhuận {s} tăng trưởng so với cùng kỳ."),
    ("{s}: Thanh khoản tăng mạnh", "Dòng tiền đổ vào {s} trong phiên giao dịch."),
    ("Cổ phiếu {s} được khuyến nghị mua", "Công ty chứng khoán nâng giá mục tiêu cho {s}."),
    ("{s} chốt quyền trả cổ tức", "{s} thông báo lịch chi trả cổ tức bằng tiền mặt."),
]


def extract_news_data(symbol):
    """Sinh tin tức tổng hợp (nguồn miễn phí không có tin tức cổ phiếu VN).

    public_date dạng 'YYYY-MM-DD HH:MM:SS' để Flink TRY_CAST sang TIMESTAMP cho job join.
    """
    try:
        results = []
        for _ in range(random.randint(1, 2)):
            title_tpl, content_tpl = random.choice(NEWS_TEMPLATES)
            change = round(random.uniform(-3, 3), 2)
            results.append({
                "symbol": symbol,
                "id": str(uuid.uuid4()),
                "news_title": title_tpl.format(s=symbol),
                "news_source_link": f"https://example.com/news/{symbol.lower()}",
                "public_date": datetime.now(vietnamese_timezone).strftime("%Y-%m-%d %H:%M:%S"),
                "news_short_content": content_tpl.format(s=symbol),
                "close_price": round(random.uniform(15000, 80000), 2),
                "price_change_pct": change,
            })
        return results
    except Exception as e:
        logger.error(f"Build news failed: {e}")
        return None
###------kafka-----####
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


def kafka_producer_news(symbols,producer):
    try:  
        for symbol in symbols:
            logger.info(f"Producing messages for symbols: {symbol}")
            records = extract_news_data(symbol)
            if not records:
                logger.warning(f"No news records for symbol {symbol}")
                continue
            for record in records:
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
    kafka_producer_news(SYMBOLS, producer)
    #schedule
    schedule.every(120).seconds.do(kafka_producer_news,SYMBOLS,producer)
    while True:
        schedule.run_pending()