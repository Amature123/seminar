"""
Flink streaming job — khai thác Flink sâu hơn cho pipeline chứng khoán VN30.

Một job duy nhất (gộp nhiều INSERT bằng STATEMENT SET để dùng chung source scan)
gồm 4 nhóm tính năng:

  1. CASCADING WINDOWS  — nến 1m -> 5m/15m/30m/1H tính dồn từ 1m (thay vì gộp lại
     từ raw nhiều lần). Ghi vào topic `flink_computed_ohlc` (giữ nguyên contract cũ
     để consumer OHVLC_handler.py không phải đổi).

  2. TECHNICAL INDICATORS — OVER window: SMA, STDDEV/Bollinger, VWAP, return;
     cộng EMA12/EMA26/MACD/RSI14 qua Python UDF (udfs.py). -> topic `flink_indicators`.

  3. CEP / SIGNALS — MATCH_RECOGNIZE phát hiện mẫu hình (3 nến tăng/giảm liên tiếp,
     volume spike, breakout). -> topic `flink_signals`.

  4. MULTI-STREAM JOIN — interval join luồng news (`ticks_symbol`) với luồng giá để
     đo biến động giá sau tin. -> topic `flink_news_impact` (upsert).

LƯU Ý: môi trường viết code này không có Flink runtime nên job CHƯA chạy thử end-to-end.
Xem checklist kiểm thử ở cuối file / trong phần bàn giao.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = "kafka_broker:19092,kafka_broker_1:19092,kafka_broker_2:19092"


def build_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    cfg = t_env.get_config()
    cfg.set("pipeline.name", "vn30-flink-analytics")
    cfg.set("parallelism.default", "3")
    cfg.set("table.exec.source.idle-timeout", "10s")
    # Python UDF chạy ở chế độ in-process cho gọn (process mode mặc định cũng được)
    cfg.set("python.execution-mode", "process")
    return t_env


# --------------------------------------------------------------------------- #
# SOURCES
# --------------------------------------------------------------------------- #
def create_sources(t_env: TableEnvironment) -> None:
    # Raw OHLC 1m thô do producer OHVLC.py đẩy lên (append-only, có watermark).
    t_env.execute_sql(f"""
        CREATE TABLE ohlc_source (
            screener STRING,
            symbol   STRING,
            `open`   DOUBLE,
            `high`   DOUBLE,
            `low`    DOUBLE,
            `close`  DOUBLE,
            `volume` DOUBLE,
            `time`   TIMESTAMP(3),
            WATERMARK FOR `time` AS `time` - INTERVAL '10' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ohvcl_data',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'flink_analytics_ohlc',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'scan.watermark.idle-timeout' = '10sec'
        )
    """)

    # Luồng tin tức (producer ticks_news.py -> topic ticks_symbol).
    # Dùng public_date làm event-time; TRY_CAST để dữ liệu lỗi định dạng bị bỏ qua
    # thay vì làm hỏng job.
    t_env.execute_sql(f"""
        CREATE TABLE news_source (
            symbol   STRING,
            id       STRING,
            news_title STRING,
            news_source_link STRING,
            public_date STRING,
            news_short_content STRING,
            close_price DOUBLE,
            price_change_pct DOUBLE,
            event_time AS TRY_CAST(public_date AS TIMESTAMP(3)),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ticks_symbol',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'flink_analytics_news',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)


# --------------------------------------------------------------------------- #
# SINKS
# --------------------------------------------------------------------------- #
def create_sinks(t_env: TableEnvironment) -> None:
    # (1) OHLC đa khung — giữ nguyên contract cũ (upsert-kafka, PK screener+symbol+time)
    t_env.execute_sql(f"""
        CREATE TABLE ohlc_kafka_sink (
            screener STRING,
            symbol   STRING,
            `time`   TIMESTAMP(3),
            `open`   DOUBLE,
            `high`   DOUBLE,
            `low`    DOUBLE,
            `close`  DOUBLE,
            `volume` DOUBLE,
            PRIMARY KEY (screener, symbol, `time`) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'flink_computed_ohlc',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'key.format' = 'json',
            'value.format' = 'json',
            'sink.delivery-guarantee' = 'at-least-once',
            'sink.transactional-id-prefix' = 'tx-ohlc'
        )
    """)

    # (2) Indicators — append stream
    t_env.execute_sql(f"""
        CREATE TABLE indicators_sink (
            screener STRING,
            symbol   STRING,
            `time`   TIMESTAMP(3),
            `close`  DOUBLE,
            sma20    DOUBLE,
            bb_mid   DOUBLE,
            bb_upper DOUBLE,
            bb_lower DOUBLE,
            vwap     DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flink_indicators',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format' = 'json',
            'sink.delivery-guarantee' = 'at-least-once',
            'sink.transactional-id-prefix' = 'tx-ind'
        )
    """)

    # (3) Signals (CEP) — append stream
    t_env.execute_sql(f"""
        CREATE TABLE signals_sink (
            symbol      STRING,
            signal_time TIMESTAMP(3),
            signal_type STRING,
            side        STRING,
            price       DOUBLE,
            detail      STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flink_signals',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format' = 'json',
            'sink.delivery-guarantee' = 'at-least-once',
            'sink.transactional-id-prefix' = 'tx-sig'
        )
    """)

    # (4) News impact — updating -> upsert-kafka theo (symbol, id)
    t_env.execute_sql(f"""
        CREATE TABLE news_impact_sink (
            symbol     STRING,
            id         STRING,
            news_time  TIMESTAMP(3),
            title      STRING,
            link       STRING,
            price_before DOUBLE,
            price_after  DOUBLE,
            max_price    DOUBLE,
            min_price    DOUBLE,
            n_candles    BIGINT,
            impact_pct   DOUBLE,
            PRIMARY KEY (symbol, id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'flink_news_impact',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'key.format' = 'json',
            'value.format' = 'json',
            'sink.delivery-guarantee' = 'at-least-once',
            'sink.transactional-id-prefix' = 'tx-news'
        )
    """)


# --------------------------------------------------------------------------- #
# VIEWS — nến 1m sạch (window TVF), làm gốc cho cascading + indicators + CEP
# --------------------------------------------------------------------------- #
def create_views(t_env: TableEnvironment) -> None:
    # Nến 1m chuẩn hoá: producer có thể gửi nhiều bản ghi/phút -> TUMBLE 1m dedup.
    # window_time là rowtime, dùng được cho cascading window & OVER & MATCH_RECOGNIZE.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW ohlc_1m AS
        SELECT
            symbol,
            window_time AS rt,
            FIRST_VALUE(`open`)  AS `open`,
            MAX(`high`)          AS `high`,
            MIN(`low`)           AS `low`,
            LAST_VALUE(`close`)  AS `close`,
            SUM(`volume`)        AS `volume`
        FROM TABLE(
            TUMBLE(TABLE ohlc_source, DESCRIPTOR(`time`), INTERVAL '1' MINUTE)
        )
        GROUP BY symbol, window_start, window_end, window_time
    """)

    # Chỉ báo kỹ thuật trên nến 1m bằng OVER window.
    # LƯU Ý 2 giới hạn Flink streaming:
    #   (1) mỗi SELECT chỉ dùng MỘT loại OVER window -> tách chuỗi view, mỗi tầng 1 window.
    #   (2) KHÔNG hỗ trợ bounded ROWS (ROWS BETWEEN n PRECEDING) -> chỉ UNBOUNDED hoặc
    #       time-RANGE. Nến 1m nên RANGE '20' MINUTE ~ 20 nến.
    # EMA/RSI/MACD (đệ quy) được tính ở consumer (indicator_handler.py).
    # MỘT view, MỘT window duy nhất (tất cả aggregate cùng spec) -> tránh cả lỗi
    # "same window" lẫn "physical offsets" do chain OVER. VWAP = rolling 20'.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW ohlc_1m_ind AS
        SELECT symbol, rt AS `time`, `close`,
               AVG(`close`) OVER w AS sma20,
               AVG(`close`) OVER w AS bb_mid,
               AVG(`close`) OVER w + 2 * STDDEV_POP(`close`) OVER w AS bb_upper,
               AVG(`close`) OVER w - 2 * STDDEV_POP(`close`) OVER w AS bb_lower,
               CASE WHEN SUM(`volume`) OVER w > 0
                    THEN SUM(`close` * `volume`) OVER w / SUM(`volume`) OVER w
                    ELSE NULL END AS vwap
        FROM ohlc_1m
        WINDOW w AS (PARTITION BY symbol ORDER BY rt
                     RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
    """)


# --------------------------------------------------------------------------- #
# INSERT statements
# --------------------------------------------------------------------------- #
def cascading_ohlc_insert():
    """5m/15m/30m/1H tính DỒN từ nến 1m (cascading window).

    GỘP tất cả khung vào MỘT insert bằng UNION ALL -> một sink operator duy nhất
    (nhiều INSERT vào cùng 1 sink sẽ trùng transactional-id-prefix).
    """
    parts = ["""
        SELECT '1m' AS screener, symbol, rt AS `time`,
               `open`, `high`, `low`, `close`, `volume`
        FROM ohlc_1m
    """]
    for label, minutes in [("5m", "5"), ("15m", "15"), ("30m", "30"), ("1H", "60")]:
        parts.append(f"""
            SELECT
                '{label}' AS screener,
                symbol,
                window_time AS `time`,
                FIRST_VALUE(`open`) AS `open`,
                MAX(`high`)         AS `high`,
                MIN(`low`)          AS `low`,
                LAST_VALUE(`close`) AS `close`,
                SUM(`volume`)       AS `volume`
            FROM TABLE(
                TUMBLE(TABLE ohlc_1m, DESCRIPTOR(rt), INTERVAL '{minutes}' MINUTE)
            )
            GROUP BY symbol, window_start, window_end, window_time
        """)
    return "INSERT INTO ohlc_kafka_sink\n" + "\nUNION ALL\n".join(parts)


def indicators_insert():
    return """
        INSERT INTO indicators_sink
        SELECT
            '1m' AS screener,
            symbol,
            `time`,
            `close`,
            sma20,
            bb_mid,
            bb_upper,
            bb_lower,
            vwap
        FROM ohlc_1m_ind
    """


def signal_insert():
    """CEP bằng MATCH_RECOGNIZE: 3 nến tăng/giảm liên tiếp + volume spike.

    GỘP 3 nhánh vào MỘT insert (UNION ALL) -> một sink operator (tránh trùng prefix).
    """
    bullish = """
        SELECT symbol, CAST(signal_time AS TIMESTAMP(3)) AS signal_time,
               'TREND_3UP' AS signal_type, 'BUY' AS side, price,
               'close tang 3+ nen lien tiep' AS detail
        FROM ohlc_1m
        MATCH_RECOGNIZE (
            PARTITION BY symbol
            ORDER BY rt
            MEASURES
                C.rt      AS signal_time,
                C.`close` AS price
            ONE ROW PER MATCH
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (A B C)
            DEFINE
                B AS B.`close` > A.`close`,
                C AS C.`close` > B.`close`
        )
    """
    bearish = """
        SELECT symbol, CAST(signal_time AS TIMESTAMP(3)) AS signal_time,
               'TREND_3DOWN' AS signal_type, 'SELL' AS side, price,
               'close giam 3+ nen lien tiep' AS detail
        FROM ohlc_1m
        MATCH_RECOGNIZE (
            PARTITION BY symbol
            ORDER BY rt
            MEASURES
                C.rt      AS signal_time,
                C.`close` AS price
            ONE ROW PER MATCH
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (A B C)
            DEFINE
                B AS B.`close` < A.`close`,
                C AS C.`close` < B.`close`
        )
    """
    # Volume spike: 1 nến có volume > 3x trung bình 20' trước đó.
    spike = """
        SELECT symbol, CAST(`time` AS TIMESTAMP(3)) AS signal_time,
               'VOLUME_SPIKE' AS signal_type, 'WATCH' AS side, `close` AS price,
               CONCAT('volume gap ', CAST(ROUND(`volume` / NULLIF(avg_vol, 0), 2) AS STRING), 'x') AS detail
        FROM (
            SELECT symbol, rt AS `time`, `close`, `volume`,
                   AVG(`volume`) OVER (
                       PARTITION BY symbol ORDER BY rt
                       RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW
                   ) AS avg_vol
            FROM ohlc_1m
        )
        WHERE avg_vol IS NOT NULL AND `volume` > 3 * avg_vol
    """
    return "INSERT INTO signals_sink\n" + "\nUNION ALL\n".join([bullish, bearish, spike])


def news_impact_insert():
    """
    Interval join: với mỗi tin, gom các nến 1m trong [news, news+15m] để đo biến
    động giá sau tin. Group agg -> updating -> upsert theo (symbol, id).
    """
    return """
        INSERT INTO news_impact_sink
        SELECT
            n.symbol,
            n.id,
            n.event_time AS news_time,
            n.news_title AS title,
            n.news_source_link AS link,
            FIRST_VALUE(o.`close`) AS price_before,
            LAST_VALUE(o.`close`)  AS price_after,
            MAX(o.`high`)          AS max_price,
            MIN(o.`low`)           AS min_price,
            COUNT(*)               AS n_candles,
            CASE WHEN FIRST_VALUE(o.`close`) > 0
                 THEN (LAST_VALUE(o.`close`) - FIRST_VALUE(o.`close`))
                      / FIRST_VALUE(o.`close`) * 100
                 ELSE NULL END AS impact_pct
        FROM news_source AS n
        JOIN ohlc_source AS o
          ON n.symbol = o.symbol
         AND o.`time` BETWEEN n.event_time AND n.event_time + INTERVAL '15' MINUTE
        GROUP BY n.symbol, n.id, n.event_time, n.news_title, n.news_source_link
    """


# --------------------------------------------------------------------------- #
def main():
    t_env = build_env()
    create_sources(t_env)
    create_sinks(t_env)
    create_views(t_env)

    stmt = t_env.create_statement_set()
    stmt.add_insert_sql(cascading_ohlc_insert())
    stmt.add_insert_sql(indicators_insert())
    stmt.add_insert_sql(signal_insert())
    stmt.add_insert_sql(news_impact_insert())

    stmt.execute()


if __name__ == "__main__":
    main()
