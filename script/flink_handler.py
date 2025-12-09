from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.typeinfo import Types
from pyflink.common.time import Duration
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction, KeyedProcessFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, Time
from datetime import datetime
from pyflink.datastream.connectors.cassandra import CassandraSink 
import json 

import logging    
from collections import deque
from pyflink.common import Configuration
from pyflink.datastream.state import MapStateDescriptor
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class StockTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        data = json.loads(event)
        dt = datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp() * 1000)


# ============================================================================
# USE CASE 1: TIME-BASED AGGREGATIONS (OHLC Resampling) - REALTIME
# ============================================================================


# class OHLCRealtimeProcessor(KeyedProcessFunction):
#     def open(self, runtime_context):
#         from pyflink.datastream.state import ValueStateDescriptor
        
#         self.state_1min = runtime_context.get_state(
#             ValueStateDescriptor("ohlc_1min", Types.PICKLED_BYTE_ARRAY())
#         )
#         self.state_5min = runtime_context.get_state(
#             ValueStateDescriptor("ohlc_5min", Types.PICKLED_BYTE_ARRAY())
#         )
#         self.state_15min = runtime_context.get_state(
#             ValueStateDescriptor("ohlc_15min", Types.PICKLED_BYTE_ARRAY())
#         )
#     def process_element(self, value, ctx):
#         try:
#             data = json.loads(value) if isinstance(value, str) else value
#             symbol = data['symbol']
#             price = float(data['close'])
#             high = float(data['high'])
#             low = float(data['low'])
#             volume = float(data.get('volume', 0))
#             current_time = ctx.timestamp()
#             timeframes = [
#                 ('1min', 60 * 1000, self.state_1min),
#                 ('5min', 5 * 60 * 1000, self.state_5min),
#                 ('15min', 15 * 60 * 1000, self.state_15min)
#             ]
#             for tf_name, tf_ms, state in timeframes:
#                 window_start = (current_time // tf_ms) * tf_ms
#                 window = state.value()
#                 if window is None or window.get('start_time') != window_start:
#                     if window is not None:
#                         result = {
#                             'symbol': symbol,
#                             'timeframe': tf_name,
#                             'open': round(window['open'], 2),
#                             'high': round(window['high'], 2),
#                             'low': round(window['low'], 2),
#                             'close': round(window['close'], 2),
#                             'volume': round(window['volume'], 2),
#                             'tick_count': window['count'],
#                             'type': 'ohlc_realtime'
#                         }
#                         yield json.dumps(result)
#                     window = {
#                         'open': price,
#                         'high': high,
#                         'low': low,
#                         'close': price,
#                         'volume': volume,
#                         'start_time': window_start,
#                         'count': 1
#                     }
#                 else:
#                     window['high'] = max(window['high'], high)
#                     window['low'] = min(window['low'], low)
#                     window['close'] = price
#                     window['volume'] += volume
#                     window['count'] += 1
#                 state.update(window)
                
#         except Exception as e:
#             logger.error(f"Error processing element: {e}")

class FlinkStockHandler:
    def __init__(self, topic_in, topic_out, kafka_servers):
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.kafka_servers = kafka_servers
        config = Configuration()
        config.set_string("jobmanager.rpc.address", "jobmanager")
        config.set_string("jobmanager.rpc.port", "6123")
        config.set_string("rest.port", "8081")
        config.set_string("pipeline.jars","file:///opt/flink/lib/flink-connector-kafka-4.0.0-2.0.jar;file:///opt/flink/opt/flink-python-2.0.0.jar")

        config.set_string("python.fn-execution.bundle.size", "1000")
        self.env = StreamExecutionEnvironment.get_execution_environment(config)

        self.env.set_parallelism(1)
        
        self.env.enable_checkpointing(5000)

        self.source = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.kafka_servers)
            .set_topics(self.topic_in)
            .set_group_id("stock_group")
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        
        self.watermark_strategy = (
            WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(3))
            .with_timestamp_assigner(StockTimestampAssigner())
        )
        
        # Kafka Sink
        self.sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.kafka_servers)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self.topic_out)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
        )
    
    def process_all_use_cases(self):
        """Run all 6 use cases optimized for REALTIME streaming"""
        logger.info("Starting REALTIME stock processing pipeline...")
        
        # Base stream
        base_stream = ( self.env.from_source(
            self.source,
            self.watermark_strategy,
            "Kafka Source"
            )
        .map(lambda x: json.loads(x))
        )

        ohlc_stream = (
            base_stream
            .key_by(lambda record:record['symbol'])
            .process(OHLCRealtimeProcessor())
            .map(lambda x: json.dumps(x))
        )
        logger.info("✅ OHLC Realtime processing set up.")

        # Sink to Kafka
        ohlc_stream.sink_to(self.sink)
        
        logger.info("✅ Executing REALTIME stock processing pipeline...")
        self.env.execute("Realtime Stock Analysis Pipeline")
        logger.info("✅ REALTIME stock processing pipeline execution finished.")

if __name__ == "__main__":
    handler = FlinkStockHandler(
        topic_in='stock_data',
        topic_out='stock_processed',
        kafka_servers='kafka_broker:19092'
    )
    handler.process_all_use_cases()
    logger.info("Flink Stock Handler execution completed.")