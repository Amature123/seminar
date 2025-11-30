CREATE TABLE stock_stream (
  symbol STRING,
  time TIMESTAMP(3),
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  WATERMARK FOR time AS time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'stock_data',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'stock-group',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
CREATE TABLE print_sink (
  symbol STRING,
  avg_close DOUBLE,
  max_high DOUBLE,
  min_low DOUBLE,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);
INSERT INTO print_sink
SELECT
  symbol,
  AVG(close) AS avg_close,
  MAX(high) AS max_high,
  MIN(low) AS min_low,
  window_start,
  window_end
FROM TABLE(
  TUMBLE(TABLE stock_stream, DESCRIPTOR(time), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;


        self.env.add_jars("file:///opt/flink/opt/flink-connector-kafka-4.0.0-2.0.jar","file:///opt/flink/opt/kafka-clients-4.0.0.jar")