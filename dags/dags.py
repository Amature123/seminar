from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
import uuid

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

from script.Kafkaproduction import KafkaUserDataProducer

def produce_stock_data(**kwargs):
    producer = KafkaUserDataProducer()
    stock_data = {
        "symbol": "AAPL",
        "price": 150.0,
        "timestamp": datetime.now()
    }
    producer.produce(stock_data)

with DAG(
    'stock_data_producer_dag',
    default_args=default_args,
    description='A DAG to produce stock data to Kafka every minute',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:

    produce_task = PythonOperator(
        task_id='produce_stock_data',
        python_callable=produce_stock_data,
        provide_context=True,
    )

    produce_task
