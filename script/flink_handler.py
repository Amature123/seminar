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
from urllib import response
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults 
import requests
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class FlinkSqlOperator(BaseOperator):
    @apply_defaults 
    def __init__(self, flink_host, sql_statement, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flink_host = flink_host
        self.sql_statement = sql_statement

    def get_session_id(self):
        logger.info(f"Creating Flink SQL session on host: {self.flink_host}")
        response = requests.post(f'http://{self.flink_host}:8083/v1/sessions', json={})
        response.raise_for_status()
        session_info = response.json()
        return session_info['sessionHandle']

    def statement_execute(self, session_id):
        logger.info(f"Executing SQL statement in session ID: {session_id}")
        payload = {
            "statement": self.sql_statement
        }
        response = requests.post(f'http://{self.flink_host}:8083/v1/sessions/{session_id}/statements', json=payload)
        response.raise_for_status()
        operation_handler = response.json()
        if response.status_code != 200:
            raise Exception(f"Flink SQL failed: {response.text}")
        return operation_handler['operationHandle']
    
    def get_statement_result(self, session_id, operation_handle):
        logger.info(f"Fetching result for operation handle: {operation_handle} in session ID: {session_id}")
        response = requests.get(f'http://{self.flink_host}:8083/v1/sessions/{session_id}/operations/{operation_handle}/result/0')
        response.raise_for_status()
        result = response.json()
        while result['resultType']!='EOS':
            if result['resultType']=='NOT_READY':
                logger.info("Result not ready, waiting...")
                continue
            logger.info(f"show result : {result}, fetching next chunk...")
            next_response = requests.get(f'http://{self.flink_host}:8083{result["nextResultUri"]}')
            next_response.raise_for_status()
            result = next_response.json()
            logger.info(f"Intermediate Flink SQL result: {result}")
        logger.info(f"Flink SQL final result: {result}")
        return result

    def execute(self, context):
        session_id = self.get_session_id()
        operation_handle = self.statement_execute(session_id)
        result = self.get_statement_result(session_id, operation_handle)
        logger.info(f"Flink SQL execution result: {result}")
        return result