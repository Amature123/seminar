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
  'topic' = 'flink-input',
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
