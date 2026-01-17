

CREATE OR REPLACE TABLE ohlcv_flink_source (
  screener STRING,
  symbol STRING,
  `time` TIMESTAMP(3),
  `open` DOUBLE,
  `high` DOUBLE,
  `low` DOUBLE,
  `close` DOUBLE,
  `volume` DOUBLE,
  `topic_partition` INT METADATA FROM 'partition',
  WATERMARK FOR `time` AS `time` - INTERVAL '5' SECONDS 
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink_computed_ohlc',
  'properties.bootstrap.servers' = 'kafka_broker:19092,kafka_broker_1:19092,kafka_broker_2:19092',
  'properties.group.id' = 'metrics_key',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'scan.watermark.idle-timeout'='10sec'
);

CREATE OR REPLACE VIEW sma_20 AS
SELECT symbol, `close`, `time`,
    AVG(`close`) OVER (PARTITION BY symbol ORDER BY `time` ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20
FROM ohlcv_flink_source;

CREATE OR REPLACE VIEW sma_50 AS
SELECT
    symbol,
    `close`,
    `time`,
    AVG(`close`) OVER (PARTITION BY symbol ORDER BY `time` ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50
FROM ohlcv_flink_source;

