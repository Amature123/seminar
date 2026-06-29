import logging
import time
from collections import defaultdict, deque
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from utils import safe_json_deserializer, transform_time

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("indicator_consumer")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
TOPIC = 'flink_indicators'
GROUP_ID = 'indicator-consumer-group'

# Lịch sử close theo từng symbol để tính các chỉ báo đệ quy (EMA/RSI/MACD).
# Flink streaming SQL không tính được các chỉ báo này (đệ quy + cần bounded ROWS),
# nên tính ở đây bằng đúng công thức trong flink/job/udfs.py.
HIST = defaultdict(lambda: deque(maxlen=300))


def ema_value(prices, span):
    if not prices or span <= 0:
        return None
    e = prices[0]
    alpha = 2.0 / (span + 1.0)
    for p in prices[1:]:
        e = alpha * p + (1.0 - alpha) * e
    return float(e)


def rsi_value(prices, period=14):
    if len(prices) < period + 1:
        return None
    gains, losses = [], []
    for prev, cur in zip(prices[:-1], prices[1:]):
        ch = cur - prev
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


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
        INSERT INTO market.indicators (
            screener, symbol, time,
            close, sma20, ema12, ema26, macd, rsi14,
            bb_mid, bb_upper, bb_lower, vwap, ret
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)


def _f(value):
    return float(value) if value is not None else None


def insert_data(session, insert_stmt, record: dict):
    try:
        symbol = record['symbol']
        close = _f(record.get('close'))

        # Cập nhật lịch sử và tính EMA/RSI/MACD/ret
        ema12 = ema26 = macd = rsi14 = ret = None
        if close is not None:
            HIST[symbol].append(close)
            prices = list(HIST[symbol])
            ema12 = ema_value(prices, 12)
            ema26 = ema_value(prices, 26)
            if ema12 is not None and ema26 is not None:
                macd = ema12 - ema26
            rsi14 = rsi_value(prices, 14)
            if len(prices) >= 2:
                ret = prices[-1] - prices[-2]

        session.execute(insert_stmt, (
            record.get('screener', '1m'),
            symbol,
            transform_time(record['time']),
            close,
            _f(record.get('sma20')),
            ema12, ema26, macd, rsi14,
            _f(record.get('bb_mid')),
            _f(record.get('bb_upper')),
            _f(record.get('bb_lower')),
            _f(record.get('vwap')),
            ret,
        ))
        logger.info(
            f"Inserted IND {symbol} @ {record['time']} "
            f"sma20={record.get('sma20')} rsi14={rsi14}"
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
    logger.info("Start consuming indicators...")
    while True:
        try:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                time.sleep(wait)
                continue
            for tp, messages in records.items():
                for message in messages:
                    if message.value is None:
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
