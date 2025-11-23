import json
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types


class MarketNormalizer:
    def __init__(self,
                 kafka_broker="kafka:9092",
                 input_topic="market_raw",
                 output_topic="market_normalized",
                 group_id="market-normalizer"):

        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
    def transform(self, record: str):

        try:
            data = json.loads(record)
        except:
            return None

        # Validate required fields
        required = ["symbol", "price", "volume", "timestamp"]
        if not all(f in data for f in required):
            return None

        try:
            price = float(data["price"])
            volume = int(data["volume"])
            ts = int(
                datetime.fromisoformat(
                    data["timestamp"].replace("Z", "+00:00")
                ).timestamp() * 1000
            )
        except:
            return None

        normalized = {
            "symbol": data["symbol"].upper(),
            "price": price,
            "volume": volume,
            "event_time": ts,
            "source": data.get("source", "unknown")
        }

        return json.dumps(normalized)
    def dedup_key(self, record: str):
        try:
            d = json.loads(record)
            return f"{d['symbol']}_{d['event_time']}"
        except:
            return "invalid"

    def build_source(self):
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(self.kafka_broker)
            .set_group_id(self.group_id)
            .set_topics(self.input_topic)
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

    def run(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(1)

        source = self.build_source()
        sink = self.build_sink()

        stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            "market_raw_source"
        )

        processed = (
            stream
            .map(self.transform, output_type=Types.STRING())
            .filter(lambda x: x is not None)
            .key_by(self.dedup_key)
            .reduce(lambda a, b: b)
        )

        processed.sink_to(sink)

        env.execute("market_normalizer_job")


# ======================================
# MAIN ENTRY
# ======================================
if __name__ == "__main__":
    job = MarketNormalizer(
        kafka_broker="kafka:9092",
        input_topic="market_raw",
        output_topic="market_normalized",
        group_id="market-normalizer"
    )

    job.run()
