from pyflink.datastream import TimeCharacteristic
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows

def process_stream(self):
    logger.info("Starting DataStream processing...")

    try:
        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.kafka_servers)
            .set_topics(self.topic_in)
            .set_group_id("stock_group")
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

        stream = self.env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
        logger.info("Kafka source created successfully.")
    except Exception as e:
        logger.error(f"Error creating Kafka source: {e}")
        return  

    # --- Parse JSON và chuyển sang dạng object ---
    parsed_stream = stream.map(
        lambda x: json.loads(x),
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    # --- Chuyển đổi dữ liệu về dạng cụ thể ---
    mapped_stream = parsed_stream.map(
        lambda d: (d["symbol"], float(d["price"]), int(d["volume"])),
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT()])
    )

    # --- Window aggregation (5 phút) ---
    averaged_stream = (
        mapped_stream
        .key_by(lambda x: x[0])  # theo mã cổ phiếu
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
        .reduce(
            lambda a, b: (a[0], (a[1] + b[1]) / 2, a[2] + b[2]),  # tính trung bình giá, cộng volume
            output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT()])
        )
    )

    # --- Serialize lại thành JSON ---
    json_stream = averaged_stream.map(
        lambda t: json.dumps({"symbol": t[0], "avg_price": t[1], "total_volume": t[2]}),
        output_type=Types.STRING()
    )

    # --- Kafka sink ---
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(self.topic_out)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    json_stream.sink_to(sink)

    logger.info("✅ Submitting DataStream job...")
    self.env.execute("Stock Price Analytics Job")
