import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.cassandra.sink.CassandraSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetSocketAddress;
import java.sql.Timestamp;

public class OHVLC {
    public String symbol;
    public java.sql.Timestamp time;
    public double open;
    public double high;
    public double low;
    public double close;
    public double volume;

    public OHVLC() {}
}


public class KafkaToCassandraJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(
                        "kafka_broker:19092," +
                        "kafka_broker_1:19092," +
                        "kafka_broker_2:19092"
                )
                .setTopics("ohvcl_data")
                .setGroupId("ohvcl-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        ObjectMapper mapper = new ObjectMapper();

        DataStream<OHVLC> parsedStream = rawStream.map(json -> {
            JsonNode node = mapper.readTree(json);

            OHVLC o = new OHVLC();
            o.symbol = node.get("symbol").asText();
            o.time = Timestamp.valueOf(node.get("time").asText());
            o.open = node.get("open").asDouble();
            o.high = node.get("high").asDouble();
            o.low = node.get("low").asDouble();
            o.close = node.get("close").asDouble();
            o.volume = node.get("volume").asDouble();
            return o;
        });

        CassandraSink.addSink(parsedStream)
                .setQuery(
                        "INSERT INTO market.OHVLC " +
                        "(symbol, time, open, high, low, close, volume) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?);"
                )
                .setClusterBuilder(() ->
                        CqlSession.builder()
                                .addContactPoint(
                                        new InetSocketAddress("cassandra", 9042)
                                )
                                .withLocalDatacenter("datacenter1")
                                .build()
                )
                .build();

        env.execute("Kafka → Flink → Cassandra OHVLC");
    }
}
