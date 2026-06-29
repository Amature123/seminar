import logging
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from utils import safe_json_deserializer, transform_time

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("news_impact_consumer")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
TOPIC = 'flink_news_impact'
GROUP_ID = 'news-impact-consumer-group'


def connect_cassandra(max_retries=10, delay=5):
    retries = 0
    while retries < max_retries:
        cluster = None
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(KEYSPACE)
            logger.info("Connected to Cassandra")
            return session
        except NoHostAvailable:
            retries += 1
            logger.warning(
                f"Cassandra not ready (attempt {retries}/{max_retries}), retrying in {delay}s..."
            )
            time.sleep(delay)
        finally:
            if cluster and retries >= max_retries:
                cluster.shutdown()
    raise RuntimeError("Failed to connect to Cassandra after retries")


def prepare_statements(session):
    return session.prepare("""
        INSERT INTO market.news_impact (
            symbol, id, news_time, title, link,
            price_before, price_after, max_price, min_price,
            n_candles, impact_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)


def _f(value):
    return float(value) if value is not None else None


def insert_data(session, insert_stmt, record: dict):
    try:
        session.execute(insert_stmt, (
            record['symbol'],
            record['id'],
            transform_time(record['news_time']),
            record.get('title'),
            record.get('link'),
            _f(record.get('price_before')),
            _f(record.get('price_after')),
            _f(record.get('max_price')),
            _f(record.get('min_price')),
            int(record['n_candles']) if record.get('n_candles') is not None else None,
            _f(record.get('impact_pct'))
        ))
        logger.info(
            f"Inserted NEWS_IMPACT {record['symbol']} id={record['id']} "
            f"impact={record.get('impact_pct')}%"
        )
    except Exception as e:
        logger.error(f"Cassandra insert error: {e}")


def create_consumer():
    logger.info("Setting up Kafka consumer...")
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=safe_json_deserializer,
        max_poll_records=100
    )


def consume_data(session, insert_stmt, wait=5):
    consumer = create_consumer()
    logger.info("Start consuming news impact...")
    while True:
        try:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                time.sleep(wait)
                continue
            for tp, messages in records.items():
                for message in messages:
                    if message.value is None:
                        # tombstone từ upsert-kafka
                        continue
                    insert_data(session, insert_stmt, message.value)
            consumer.commit()
        except Exception as e:
            logger.error(f"Consume error: {e}")
            time.sleep(3)


if __name__ == "__main__":
    session = connect_cassandra()
    insert_stmt = prepare_statements(session)
    consume_data(session, insert_stmt)
