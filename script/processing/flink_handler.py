# pyflink_trading_consumer_optimized.py
import json
import logging
import threading
import time
from datetime import datetime
from typing import List, Tuple

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement, PreparedStatement, BatchType
from cassandra import OperationTimedOut
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import KafkaSource
from pyflink.datastream.functions import RichMapFunction, RuntimeContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pyflink_trading")

# ---- Config ----
CASSANDRA_HOSTS = ["cassandra"]  
CASSANDRA_KEYSPACE = "market"
BUFFER_SIZE = 200                 
FLUSH_INTERVAL = 5.0             
BASE_BACKOFF = 0.5                
INSERT_CQL = """
INSERT INTO trading_data (
    symbol, handle_time, ceiling, floor, reference, room_foreign,
    foreign_buy_volume, foreign_sell_volume,
    buy_1, buy_1_volume, buy_2, buy_2_volume, buy_3, buy_3_volume,
    sell_1, sell_1_volume, sell_2, sell_2_volume, sell_3, sell_3_volume,
    highest, lowest, average
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def parse_ts(ts_str: str):
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None


class CassandraBatchSink(RichMapFunction):
    def __init__(self, insert_cql: str,cassandra_hosts: List[str],keyspace: str,buffer_size: int = 100, flush_interval: float = 2.0, max_retries: int = 3):
        self.insert_cql = insert_cql
        self.cassandra_hosts = cassandra_hosts
        self.keyspace = keyspace
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries

        self.cluster = None
        self.session = None
        self.prepared: PreparedStatement = None
        self.buffer: List[Tuple] = []
        self.lock = threading.Lock()
        self._stopped = threading.Event()
        self._flusher_thread = None

    def open(self, runtime_context: RuntimeContext):
        logger.info("Opening Cassandra connection...")
        self.cluster = Cluster(self.cassandra_hosts)
        self.session = self.cluster.connect(self.keyspace)
        self.prepared = self.session.prepare(self.insert_cql)
        self.buffer = []
        self._flusher_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self._flusher_thread.start()
        logger.info("Cassandra connection opened and flusher started.")

    def map(self, value: str):
        try:
            data = json.loads(value)
        except Exception as e:
            logger.error("Invalid JSON, skipping: %s ; error: %s", value, e)
            return
        bound = self._to_bound_tuple(data)
        if bound is None:
            logger.debug("Skipping record because conversion failed.")
            return
        with self.lock:
            self.buffer.append(bound)
            buf_len = len(self.buffer)
        if buf_len >= self.buffer_size:
            logger.debug("Buffer size reached %d, flushing.", buf_len)
            self._flush_buffer()

    def _to_bound_tuple(self, data: dict):
        try:
            # parse fields; handle missing keys gracefully
            handle_time = parse_ts(data.get("handle_time")) if data.get("handle_time") else None
            buy = data.get("Buy", {})
            sell = data.get("Sell", {})
            return (
                data.get("symbol"),
                handle_time,
                data.get("ceiling"),
                data.get("floor"),
                data.get("reference"),
                data.get("Room_foreign"),
                data.get("foreign_buy_volume"),
                data.get("foreign_sell_volume"),
                buy.get("Buy_1"),
                buy.get("Buy_1_volume"),
                buy.get("Buy_2"),
                buy.get("Buy_2_volume"),
                buy.get("Buy_3"),
                buy.get("Buy_3_volume"),
                sell.get("Sell_1"),
                sell.get("Sell_1_volume"),
                sell.get("Sell_2"),
                sell.get("Sell_2_volume"),
                sell.get("Sell_3"),
                sell.get("Sell_3_volume"),
                data.get("highest"),
                data.get("lowest"),
                data.get("average"),
            )
        except Exception as e:
            logger.exception("Error converting JSON to tuple: %s", e)
            return None

    def _periodic_flush(self):
        logger.info("Periodic flusher thread started with interval %s seconds", self.flush_interval)
        while not self._stopped.is_set():
            time.sleep(self.flush_interval)
            try:
                self._flush_buffer()
            except Exception as e:
                logger.exception("Exception in periodic flush: %s", e)
        logger.info("Periodic flusher thread exiting.")

    def _flush_buffer(self):
        # copy and clear buffer under lock
        with self.lock:
            if not self.buffer:
                return
            batch_items = self.buffer
            self.buffer = []

        # send in batches of reasonable size (Cassandra has limits)
        CHUNK = 100   # number of statements per BatchStatement
        idx = 0
        while idx < len(batch_items):
            chunk = batch_items[idx: idx + CHUNK]
            self._send_batch(chunk)
            idx += CHUNK

    def _send_batch(self, items: List[Tuple]):
        if not items:
            return

        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for it in items:
            batch.add(self.prepared.bind(it))

        attempt = 0
        backoff = BASE_BACKOFF
        while attempt <= self.max_retries:
            try:
                future = self.session.execute_async(batch)
                # wait for completion (but non-blocking overall because chunk size is limited)
                future.result(timeout=10)
                logger.info("Successfully wrote batch of %d records to Cassandra", len(items))
                return
            except Exception as e:
                attempt += 1
                logger.warning("Write batch failed attempt %d/%d: %s", attempt, self.max_retries, e)
                if attempt > self.max_retries:
                    logger.error("Dropping batch after %d attempts. Error: %s", attempt - 1, e)
                    return
                time.sleep(backoff)
                backoff *= 2

    def close(self):
        logger.info("Closing sink: flushing remaining records...")
        # stop periodic flusher
        self._stopped.set()
        if self._flusher_thread:
            self._flusher_thread.join(timeout=5)

        # final flush
        try:
            self._flush_buffer()
        except Exception as e:
            logger.exception("Error on final flush: %s", e)

        # close cassandra
        try:
            if self.session:
                self.session.shutdown()
            if self.cluster:
                self.cluster.shutdown()
            logger.info("Cassandra connection closed.")
        except Exception as e:
            logger.exception("Error closing Cassandra connection: %s", e)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka_broker:19092") \
        .set_group_id("py_trading_group") \
        .set_topics("trading_data") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=None, source_name="Kafka Trading Source")

    sink_func = CassandraBatchSink(
        insert_cql=INSERT_CQL,
        cassandra_hosts=CASSANDRA_HOSTS,
        keyspace=CASSANDRA_KEYSPACE,
        buffer_size=BUFFER_SIZE,
        flush_interval=FLUSH_INTERVAL,
        max_retries=MAX_RETRIES,
    )

    ds.map(sink_func)

    env.execute("PyFlink TradingData Consumer Optimized")


if __name__ == "__main__":
    main()
