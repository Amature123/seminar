import logging
import time
from script.production import KafkaUserDataProducer
from script.rawDataExtraction import RawDataExtraction
from script.flink_handler import FlinkStockHandler

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def produce_stock_data():
    """Send stock price data to Kafka topic 'stock_data'."""
    producer = KafkaUserDataProducer(
        topic='stock_data',
        bootstrap_servers='kafka:9092'
    )
    symbols = ['tcb', 'vci', 'vcb', 'acb', 'tpb']
    logger.info("Starting Kafka producer for symbols: %s", symbols)
    producer.producer_loop(symbols, sleep_time=5)

def consume_stock_data():
    """Consume processed data from Kafka and write to Cassandra."""
    consumer = RawDataExtraction(
        bootstrap_servers='kafka:9092',
        group_id='raw_data',
        topic='stock_data',
        cassandra_host='cassandra'
    )
    logger.info("Starting Kafka consumer to store data in Cassandra")
    consumer.consumer_stock_price()

def processing_data():
    """Run Flink job to process stream data."""
    processer = FlinkStockHandler(
        topic_in='stock_data',
        topic_out='stock_processed',
        kafka_servers='kafka:9092'
    )
    logger.info("Starting Flink stream processing job")
    processer.process_stream()

def main():
    """
    Entry point for the full pipeline.
    Adjust the sequence if you want them to run concurrently instead of sequentially.
    """
    logger.info("=== Starting Stock Streaming Pipeline ===")

    # Step 1: Start producer (simulate streaming data)
    produce_stock_data()

    # Optional: wait a bit before starting processing
    time.sleep(3)

    # Step 2: Start Flink processing job
    processing_data()

    # Step 3 (optional): Start consumer to write results to Cassandra
    # Uncomment if you want consumer active as part of pipeline
    # consume_stock_data()

    logger.info("=== Pipeline finished (or running continuously) ===")

if __name__ == "__main__":
    main()