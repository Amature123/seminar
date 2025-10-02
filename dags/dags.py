from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
import uuid
from script.production import KafkaUserDataProducer
from confluent_kafka import Producer
from script.rawDataExtraction import rawDataExtraction
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'seminar',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

def produce_stock_data():
    producer = KafkaUserDataProducer(topic='stock_data', bootstrap_servers='localhost:9092')
    symbols = ['VCB']
