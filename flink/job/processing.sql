DROP TABLE IF EXISTS ohlc_source;
DROP TABLE IF EXISTS ohlc_kafka_sink;

CREATE TABLE ohlc_source (
  screener STRING,
  symbol STRING,
  `open` DOUBLE,
  `high` DOUBLE,
  `low` DOUBLE,
  `close` DOUBLE,
  `volume` DOUBLE,
  `time` TIMESTAMP(3),
  `topic_partition` INT METADATA FROM 'partition',
  WATERMARK FOR `time` AS `time` - INTERVAL '5' SECONDS  
) WITH (
  'connector' = 'kafka',
  'topic' = 'ohvcl_data',
  'properties.bootstrap.servers' = 'kafka_broker:19092,kafka_broker_1:19092,kafka_broker_2:19092',
  'properties.group.id' = 'test_key',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'scan.watermark.idle-timeout'='10sec'
);


CREATE TABLE ohlc_kafka_sink (
  screener STRING,
  symbol STRING,
  `time` TIMESTAMP(3),
  `open` DOUBLE,
  `high` DOUBLE,
  `low` DOUBLE,
  `close` DOUBLE,
  `volume` DOUBLE,
  PRIMARY KEY (screener, symbol,`time`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'flink_computed_ohlc',
  'properties.bootstrap.servers' = 'kafka_broker:19092,kafka_broker_1:19092,kafka_broker_2:19092',
  'key.format' = 'json',
  'value.format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'flink_ohlc',
  'properties.transaction.timeout.ms' = '600000'
);

-- CREATE TABLE print_source (
--   screener STRING,
--   symbol STRING,
--   `open` DOUBLE,
--   `high` DOUBLE,
--   `low` DOUBLE,
--   `close` DOUBLE,
--   `volume` DOUBLE,
--   `time` STRING
-- ) WITH (
--   'connector' = 'print'
-- );

-- INSERT INTO print_source
-- SELECT screener, symbol, `open`, `high`, `low`, `close`, `volume`, `time`
-- FROM ohlc_source;

-- SELECT  symbol,
--         topic_partition,
--         `time`,
--         window_start,
--         window_end
-- FROM TABLE(
--             TUMBLE(TABLE ohlc_source,
--                     DESCRIPTOR(`time`),
--                     INTERVAL '2' MINUTE)
--             );


INSERT INTO ohlc_kafka_sink
SELECT
  screener ,
  symbol ,
  `time` ,
  `open` ,
  `high` ,
  `low` ,
  `close` ,
  `volume` 
FROM ohlc_source

UNION ALL

SELECT
  '5m' AS screener,
  symbol,
  window_start AS `time`,
  FIRST_VALUE(`open`) AS `open`,
  MAX(`high`) AS `high`,
  MIN(`low`) AS `low`,
  LAST_VALUE(`close`) AS `close`,
  SUM(`volume`) AS `volume`
FROM TABLE(
  TUMBLE(
    TABLE ohlc_source,
    DESCRIPTOR(`time`),
    INTERVAL '5' MINUTE
  )
)
GROUP BY symbol, window_start

UNION ALL

SELECT
  '15m' AS screener,
  symbol,
  window_start AS `time`,
  FIRST_VALUE(`open`) AS `open`,
  MAX(`high`) AS `high`,
  MIN(`low`) AS `low`,
  LAST_VALUE(`close`) AS `close`,
  SUM(`volume`) AS `volume`
FROM TABLE(
  TUMBLE(
    TABLE ohlc_source,
    DESCRIPTOR(`time`),
    INTERVAL '15' MINUTE
  )
)
GROUP BY symbol, window_start

UNION ALL

SELECT
  '30m' AS screener,
  symbol,
  window_start AS `time`,
  FIRST_VALUE(`open`) AS `open`,
  MAX(`high`) AS `high`,
  MIN(`low`) AS `low`,
  LAST_VALUE(`close`) AS `close`,
  SUM(`volume`) AS `volume`
FROM TABLE(
  TUMBLE(
    TABLE ohlc_source,
    DESCRIPTOR(`time`),
    INTERVAL '30' MINUTE
  )
)
GROUP BY symbol, window_start

UNION ALL

SELECT
  '1H' AS screener,
  symbol,
  window_start AS `time`,
  FIRST_VALUE(`open`) AS `open`,
  MAX(`high`) AS `high`,
  MIN(`low`) AS `low`,
  LAST_VALUE(`close`) AS `close`,
  SUM(`volume`) AS `volume`
FROM TABLE(
  TUMBLE(
    TABLE ohlc_source,
    DESCRIPTOR(`time`),
    INTERVAL '1' HOUR
  )
)
GROUP BY symbol, window_start;
