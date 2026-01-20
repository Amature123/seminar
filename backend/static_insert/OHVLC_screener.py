## Create some ticker for visualization
import logging
import time
import numpy as np
from datetime import datetime,timedelta
from cassandra.cluster import Cluster, NoHostAvailable
from vnstock import Quote
from utils import SYMBOLS
from zoneinfo import ZoneInfo
import pandas as pd
vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

end_dt = datetime.now(vietnamese_timezone)
start_dt = end_dt - timedelta(days=0.5)
#
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("ohlvc_batch_once")

logger.info(f"Symbols count:{len(SYMBOLS)}")

CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
ROLLUP_RULES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1H": "1h",
}
ROWS_PER_INTERVAL = 300

def connect_cassandra(cassandra_hosts, keyspace):
    while True:
        try:
            cluster = Cluster(cassandra_hosts)
            session = cluster.connect(keyspace)
            logger.info("Connected to Cassandra")
            return session
        except NoHostAvailable:
            logger.warning("Cassandra not ready, retrying in 5s...")
            time.sleep(5)

def prepare_statements(session):
    return session.prepare("""
        INSERT INTO market.ohlvc(
            screener,
            symbol,
            time,
            open,
            high,
            low,
            close,
            volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

def fetch_1m(symbol):
    end_dt = datetime.now(vietnamese_timezone)
    start_dt = end_dt - timedelta(days=0.5)
    start = start_dt.strftime("%Y-%m-%d")
    end = end_dt.strftime("%Y-%m-%d")
    try:
        quote = Quote(symbol=symbol, source="vci")
        df = quote.history(
            start=start,
            end=end,
            interval="1m"
        )

        if df is None or df.empty:
            return None

        df["time"] = pd.to_datetime(df["time"])
        return df

    except Exception as e:
        logger.error(f"Fetch 1m failed {symbol}: {e}")
        return None

def rollup(df_1m, rule):
    df = df_1m.copy()
    df = df.set_index("time")

    rolled = (
        df
        .resample(rule, label="right", closed="right")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna()
        .tail(ROWS_PER_INTERVAL)
        .reset_index()
    )

    return rolled

def mock_check_fill_gaps(df, session, symbol, screener="1m"):
    if df.empty:
        return df
    df_start = pd.to_datetime(df["time"].min())
    df_end   = pd.to_datetime(df["time"].max())
    df_start = df_start.strftime("%Y-%m-%d")
    df_end = df_end.strftime("%Y-%m-%d")
    logger.info(f"datafram start time : {df_start}")
    logger.info(f"datafram end time : {df_end}")
    q_start = """
        SELECT time
        FROM ohlvc
        WHERE symbol = %s
          AND screener = %s
          AND time >= %s
        ORDER BY time ASC
        LIMIT 1
    """
    q_end = """
        SELECT time
        FROM ohlvc
        WHERE symbol = %s
          AND screener = %s
          AND time <= %s
        LIMIT 1
    """
    db_start_row = session.execute(
        q_start, (symbol, screener, df_start)
    ).one()
    db_end_row = session.execute(
        q_end, (symbol, screener, df_end)
    ).one()
    if not db_start_row or not db_end_row:
        return df
    db_start = db_start_row.time
    db_end   = db_end_row.time

    df_before = df[df["time"] < db_start]
    df_after  = df[df["time"] > db_end]
    return pd.concat([df_before, df_after])

def insert_batch(session, stmt, screener, symbol, df):
    for _, row in df.iterrows():
        try:
            session.execute(stmt, (
                screener,
                symbol,
                row["time"].to_pydatetime(),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
            ))
        except Exception as e:
            logger.error(f"Insert failed {symbol} {screener}: {e}")

    if not df.empty:
        logger.info(
            f"Inserted {len(df)} rows | {symbol} | {screener}"
        )

def run_once(symbols):
    session = connect_cassandra(CASSANDRA_HOSTS, KEYSPACE)
    stmt = prepare_statements(session)

    logger.info("Start OHVLC rollup batch (1m → multi timeframe)")

    for symbol in symbols:
        df_1m = fetch_1m(symbol)

        df_1m = mock_check_fill_gaps(df_1m,session,symbol)
        if df_1m is None or df_1m.empty:
            logger.info(f"No find new on static data for {symbol}, Skipping...")
            continue

        for screener, rule in ROLLUP_RULES.items():
            rolled = rollup(df_1m, rule)
            insert_batch(session, stmt, screener, symbol, rolled)

    logger.info("Rollup batch finished")


if __name__ == "__main__":
    run_once(SYMBOLS)
