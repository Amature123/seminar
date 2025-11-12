from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
from script.production import KafkaUserDataProducer
from script.rawDataExtraction import RawDataExtraction 
from script.flink_handler import FlinkStockHandler
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'seminar',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def produce_stock_data():
    producer = KafkaUserDataProducer(topic='stock_data', bootstrap_servers='kafka:9092')
    symbols = ['tcb', 'vci', 'vcb', 'acb', 'tpb']
    producer.producer_loop(symbols, sleep_time=5)


def consume_stock_data():
    consumer = RawDataExtraction(
        bootstrap_servers='kafka:9092',
        group_id='raw_data',
        topic='stock_data',
        cassandra_host='cassandra'  
    )
    consumer.consumer_stock_price()

def processing_data():
    processer = FlinkStockHandler(
        topic_in='stock_data',
        topic_out='stock_processed',
        kafka_servers='kafka:9092',
    )
    processer.process_stream()


with DAG(
    dag_id='kafka_stock_pipeline_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *', 
    catchup=False,
    tags=['kafka', 'stock', 'etl']
) as dag:
    
    produce_data_task = PythonOperator(
        task_id='produce_stock_data',
        python_callable=produce_stock_data
    )

    # consume_data_task = PythonOperator(
    #     task_id='consume_stock_data',
    #     python_callable=consume_stock_data
    # )

    flink_processing_task = PythonOperator(
        task_id =  'flink_process_stock_data',
        python_callable=processing_data
    )
    
    produce_data_task >> flink_processing_task
