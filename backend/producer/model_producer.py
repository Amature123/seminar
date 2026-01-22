import json
import logging

import time
from kafka import KafkaProducer
from pytorch_forecasting import TemporalFusionTransformer
import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import dict_factory
from datetime import datetime
from pytorch_forecasting import TemporalFusionTransformer
from utils import SYMBOLS
from zoneinfo import ZoneInfo
import schedule
import numpy as np
import json
import uuid

vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("model_handler")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]
INTERVALS={
    "1m":1,
    "5m":5,
    "15m":15,
    "30m":30
}
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
MAX_ENCODER_LENGTH = 120
MAX_PREDICTION_LENGTH = 10
MODEL_PATH = '/app/model/best_model_1.ckpt'
TOPIC = "kafka_prediction"
##########
def load_model(path: str):
    try:
        model = TemporalFusionTransformer.load_from_checkpoint(path)
        model.eval()
        logger.info(f"Loaded model from {path}")
        return model
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        raise

def df_handler(df):
    if df is None or df.empty:
        logger.error("No data found")
        return None
    df = df.set_index('time', drop=True).reset_index() 
    df = df.groupby('symbol').apply(lambda x: x.sort_values('time')).reset_index(drop=True)
    df['time_idx'] = df.groupby('symbol').cumcount()
    last_data = df[lambda x: x.time_idx == x.time_idx.max()]
    decoder_data = pd.concat(
        [
            last_data.assign(time=lambda x: x.time + pd.Timedelta(minutes=i))
            for i in range(1, MAX_PREDICTION_LENGTH + 1)
        ],
        ignore_index=True,
    )
    decoder_data["time_idx"] = range(
    df.time_idx.max() + 1,
    df.time_idx.max() + 1 + MAX_PREDICTION_LENGTH
    )
    new_prediction_data = pd.concat([df, decoder_data], ignore_index=True)
    return new_prediction_data

def prediction_data(data,model,symbol,screen):
    try : 
        raw_prediction = model.predict(
                            data,
                            mode="raw",
                            return_x=True,
                            trainer_kwargs=dict(accelerator="cpu"),
                        )
        pred = raw_prediction.output.prediction[0, :, 1] ##0.5
        pred_list =  pred.detach().cpu().numpy().tolist()

        return  {
                "symbol": symbol,
                "base_time":(data['time'].iloc[-1] - pd.Timedelta(minutes=10*screen)).isoformat(),
                "predictions": pred_list,
                "model": "TFT_v3.6",
                "created_at": datetime.now(vietnamese_timezone).isoformat(),
                "interval": screen
            }
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise

##############################
def connect_cassandra(max_retries=10, delay=5):
    retries = 0
    while retries < max_retries:
        cluster = None
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(KEYSPACE)
            session.row_factory = dict_factory
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

    raise RuntimeError("Failed to connect to Cassandra after retries")

def get_ohlvc(session, symbol, interval= "1m") -> pd.DataFrame:

    query = """
        SELECT symbol, time, open, high, low, close, volume
        FROM ohlvc
        WHERE symbol = %s AND screener = %s
        ORDER BY time DESC
        LIMIT %s
    """
    rows = session.execute(
        query,
        (symbol, interval,MAX_ENCODER_LENGTH)
    )
    df = pd.DataFrame(rows)
    data_nonpredict = df_handler(df)
    return data_nonpredict

#############################
def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks=1,
        linger_ms=10,
        value_serializer=lambda v: json.dumps(
            v, default=json_serializer
        ).encode('utf-8')
    )

def delivery_report(record_metadata):
    logger.info(
        f"Message delivered to {record_metadata.topic} "
        f"[{record_metadata.partition}] at offset {record_metadata.offset}"
    )

def json_serializer(data):
    if isinstance(data, (np.integer, np.floating)):
        return data.items()
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")

def produce_message(producer, record):
    if not record:
        return
    try:
        future = producer.send(TOPIC, value=record)
        future.add_callback(delivery_report)
        future.add_errback(
            lambda e: logger.error(f"Message delivery failed: {e}")
        )
    except Exception as e:
        logger.error(f"Error producing message: {e}")

def run_kaf(producer,session,model,symbols,intervals):
    try:  
        for symbol in symbols:
            for interval,val in intervals.items():
                logger.info(f"Producing prediction data")
                df = get_ohlvc(session,symbol,interval)
                logger.info(f"Get data final point: {df['time'].iloc[-1]}")
                record = prediction_data(df,model,symbol,val)
                produce_message(producer,record)
                logger.info(f"Record to be sent: {record}")
            producer.flush()
        logger.info("All messages sent.")
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Error in producer loop: {e}")
        time.sleep(5)
        
#########
if __name__ == "__main__":
    session = connect_cassandra()
    model = load_model(MODEL_PATH)
    producer = create_producer()
    run_kaf(producer,session,model,SYMBOLS,INTERVALS)
    schedule.every(1).minutes.do(run_kaf,producer,session,model,SYMBOLS)
    while True:
        schedule.run_pending()

