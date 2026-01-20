import json
import logging
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from datetime import datetime
from utils import safe_json_deserializer,transform_time
from zoneinfo import ZoneInfo
vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("stock_consumer")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
TOPIC = 'kafka_prediction'
GROUP_ID = 'ohvcl-consumer-group'
SCREENER = ['1m','5m', '15m', '30m', '1h']
#######################################################
def connect_cassandra(max_retries=10, delay=5):
    retries = 0
    while retries < max_retries:
        cluster = None
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(KEYSPACE)
            logger.info("Connected to Cassandra")
            return session
        except NoHostAvailable as e:
            retries += 1
            logger.warning(
                f"Cassandra not ready (attempt {retries}/{max_retries}), retrying in {delay}s..."
            )
            time.sleep(delay)
        except Exception as e:
            logger.exception("Unexpected Cassandra error")
            raise
        finally:
            if cluster and retries >= max_retries:
                cluster.shutdown()

def prepare_statements(session):
    return session.prepare("""
        INSERT INTO market.prediction (
            symbol,
            time,
            time_step,
            close_predict,
            model,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)

def insert_data(session, insert_stmt, record: dict):
    try:
        symbol = record["symbol"]
        base_time = transform_time(record["base_time"])
        created_at = transform_time(record["created_at"])
        model = record["model"]
<<<<<<< HEAD:script/processing/model_handler.py
        mean
=======
>>>>>>> 1eca910 (add backend):backend/processing/model_handler.py
        for step, value in enumerate(record["predictions"], start=1):
            session.execute(
                insert_stmt,
                (
                    symbol,
                    base_time,
                    step,
                    float(value),
                    model,
                    created_at,
                )
            )
        logger.info(
            f"Inserted prediction {symbol} @ {base_time} "
            f"({len(record['predictions'])} steps)"
        )
    except Exception as e:
        logger.exception(
            f"Cassandra insert prediction failed for {record.get('symbol')}"
        )
        raise
###################################################################

def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest", 
        value_deserializer=safe_json_deserializer,
        consumer_timeout_ms=1000
    )

def consume_data(session, insert_stmt, wait=5):
    consumer = create_consumer()
    logger.info("Start consuming data...")

    while True:
        try:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                time.sleep(wait)
                continue
            for tp, messages in records.items():
                logger.info(
                    f"Processing {len(messages)} messages "
                    f"from {tp.topic}-{tp.partition}"
                )
                for message in messages:
                    if message.value is None:
                        logger.info(
                            f"Skip tombstone message "
                            f"key={message.key}"
                        )
                        continue
                    insert_data(session, insert_stmt, message.value)
            consumer.commit()
        except Exception as e:
            logger.error(f"Consume error: {e}")
            time.sleep(3)
################################################################
if __name__ == "__main__":
    session = connect_cassandra()
    insert_stmt = prepare_statements(session)
    consume_data(session, insert_stmt)