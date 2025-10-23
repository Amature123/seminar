# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.common.serialization import SimpleStringSchema
# import logging
# import json

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)




# def process_stock_data(record):
#     logger.info(f"Processing record: {record}")
#     data = json.loads(record)
#     data["price_moving_avg"] = (data["price"] + data.get("prev_price", data["price"])) / 2
#     return json.dumps(data)

# env = StreamExecutionEnvironment.get_execution_environment()


# consumer = FlinkKafkaConsumer(
#     topics='stock_data',
#     deserialization_schema=SimpleStringSchema(),
#     properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink_stock'}
# )


# producer = FlinkKafkaProducer(
#     topic='stock_processed',
#     serialization_schema=SimpleStringSchema(),
#     producer_config={'bootstrap.servers': 'kafka:9092'}
# )

# stream = env.add_source(consumer)
# processed = stream.map(process_stock_data)
# processed.add_sink(producer)

# env.execute("Stock Streaming Job")
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests

class FlinkSqlOperator(BaseOperator):
    @apply_defaults
    def __init__(self, flink_host, sql_statement, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flink_host = flink_host
        self.sql_statement = sql_statement

    def execute(self, context):
        url = f"http://{self.flink_host}:8081/v1/statements"
        payload = {"statement": self.sql_statement}
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            raise Exception(f"Flink SQL failed: {response.text}")
        self.log.info("Flink SQL executed: %s", self.sql_statement)
