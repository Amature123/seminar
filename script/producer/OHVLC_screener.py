import logging
import time
import numpy as np
from datetime import datetime
from cassandra.cluster import Cluster, NoHostAvailable
from vnstock import Quote
from utils import SYMBOLS
from zoneinfo import ZoneInfo
import pandas as pd
vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("ohlvc_batch_once")

CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
ROLLUP_RULES = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1H": "1h",
}
ROWS_PER_INTERVAL = 10

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

def fetch_1m(symbol, minutes=300):
    """
    minutes=300 ~ đủ rollup cho 1H * 24 candles
    """
    today = datetime.now(vietnamese_timezone).strftime("%Y-%m-%d")

    try:
        quote = Quote(symbol=symbol, source="vci")
        df = quote.history(
            start=today,
            end=today,
            interval="1m"
        )

        if df is None or df.empty:
            return None

        df["time"] = pd.to_datetime(df["time"])
        return df.tail(minutes)

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

        if df_1m is None or df_1m.empty:
            continue

        for screener, rule in ROLLUP_RULES.items():
            rolled = rollup(df_1m, rule)
            insert_batch(session, stmt, screener, symbol, rolled)

    logger.info("Rollup batch finished")


if __name__ == "__main__":
    run_once(SYMBOLS)
