import json
import math
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingEventTimeWindows


class AlertEngine:

    def __init__(self,
                 kafka_broker="kafka:9092",
                 input_topic="market_normalized",
                 output_topic="market_alerts",
                 group_id="alert-engine",
                 price_threshold_pct=0.02,
                 volatility_threshold=0.015):

        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.price_threshold_pct = price_threshold_pct
        self.volatility_threshold = volatility_threshold
    def parse_record(self, record: str):
        try:
            return json.loads(record)
        except:
            return None
    def build_source(self):
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(self.kafka_broker)
            .set_topics(self.input_topic)
            .set_group_id(self.group_id)
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
    def build_sink(self):
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(self.kafka_broker)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self.output_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    def detect_price_jump(self, key, window, values):
        sorted_values = sorted(values, key=lambda x: x["event_time"])
        first = sorted_values[0]["price"]
        last = sorted_values[-1]["price"]

        change_pct = (last - first) / first

        if abs(change_pct) >= self.price_threshold_pct:
            alert = {
                "symbol": key,
                "type": "price_jump",
                "change_pct": round(change_pct, 5),
                "message": f"Price changed {round(change_pct*100, 2)}% in window",
                "timestamp": int(datetime.utcnow().timestamp() * 1000)
            }
            return json.dumps(alert)
        return None
    def detect_volatility(self, key, window, values):
        prices = [v["price"] for v in values]
        if len(prices) <= 1:
            return None

        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        volatility = math.sqrt(variance) / mean_price

        if volatility >= self.volatility_threshold:
            alert = {
                "symbol": key,
                "type": "volatility_spike",
                "volatility": round(volatility, 5),
                "window_seconds": 30,
                "message": "Volatility exceeded threshold",
                "timestamp": int(datetime.utcnow().timestamp() * 1000)
            }
            return json.dumps(alert)

        return None
    def run(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(1)

        source = self.build_source()
        sink = self.build_sink()

        raw_stream = env.from_source(
            source,
            WatermarkStrategy.for_monotonous_timestamps(),
            "market-normalized-source"
        )

        parsed = (
            raw_stream
            .map(self.parse_record, output_type=Types.PICKLED_BYTE_ARRAY())
            .filter(lambda x: x is not None)
            .assign_timestamps_and_watermarks(
                WatermarkStrategy
                .for_bounded_out_of_orderness(Duration.of_seconds(2))
                .with_timestamp_assigner(lambda e, _: e["event_time"])
            )
        )

        price_alerts = (
            parsed
            .key_by(lambda x: x["symbol"])
            .window(
                SlidingEventTimeWindows.of(
                    Duration.of_seconds(10),  # window size
                    Duration.of_seconds(2)    # slide every 2s
                )
            )
            .apply(self.detect_price_jump, output_type=Types.STRING())
            .filter(lambda x: x is not None)
        )
        volatility_alerts = (
            parsed
            .key_by(lambda x: x["symbol"])
            .window(
                SlidingEventTimeWindows.of(
                    Duration.of_seconds(30),
                    Duration.of_seconds(5)
                )
            )
            .apply(self.detect_volatility, output_type=Types.STRING())
            .filter(lambda x: x is not None)
        )

        alerts = price_alerts.union(volatility_alerts)

        alerts.sink_to(sink)

        env.execute("real_time_alert_engine")


if __name__ == "__main__":
    job = AlertEngine(
        kafka_broker="kafka:9092",
        input_topic="market_normalized",
        output_topic="market_alerts"
    )
    job.run()
