# api/main.py
from datetime import datetime, timedelta
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import dict_factory
import logging
import time
from typing import Optional, List
import pandas as pd
from zoneinfo import ZoneInfo
import streamlit as st
from lightweight_charts.widgets import StreamlitChart
from utils import SYMBOLS
vn_zone = ZoneInfo("Asia/Ho_Chi_Minh")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ohlvc_api")


CASSANDRA_HOSTS = ["cassandra"]
KEYSPACE = "market"

def get_cassandra_session():
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect(KEYSPACE)
        session.row_factory = dict_factory
        return session
    except NoHostAvailable as e:
        logger.error(f"Cassandra connection error: {e}")
        raise HTTPException(status_code=500, detail="Cassandra not available")

def get_db():
    session = get_cassandra_session()
    try:
        yield session
    finally:
        session.shutdown()

def get_latest_all_symbols(session,symbols,limit_per_symbol: int = 1):
    results = []
    for symbol in symbols:
        query = """
            SELECT symbol, time, open, high, low, close, volume
            FROM ohlvc
            WHERE symbol = %s
            LIMIT %s
        """
        rows = session.execute(query, (symbol, limit_per_symbol))
        results.extend(rows)
    return results

def get_latest_trading_all_symbols(session,symbols,limit_per_symbol: int = 1):
    results = []
    for symbol in symbols:
        query = """
            SELECT
                symbol,
                handle_time,
                ceiling,
                floor,
                reference,
                room_foreign,
                foreign_buy_volume,
                foreign_sell_volume,
                buy_1,
                buy_1_volume,
                buy_2,
                buy_2_volume,
                buy_3,
                buy_3_volume,
                sell_1,
                sell_1_volume,
                sell_2,
                sell_2_volume,
                sell_3,
                sell_3_volume,
                highest,
                lowest,
                average
            FROM trading_data
            WHERE symbol = %s
            LIMIT %s
        """
        rows = session.execute(query, (symbol, limit_per_symbol))
        results.extend(rows)
    return results
def stocks_to_str(stocks):
    return ",".join(stocks)

st.set_page_config(
    page_title="Stock peer analysis dashboard",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

"""
# :material/query_stats: Stock peer analysis

Easily compare stocks against others in their peer group.
"""
""

cols = st.columns([1, 3])
DEFAULT_STOCKS = ['ACB', 'TCB', 'FPT', 'VCB']

if "tickers_input" not in st.session_state:
    st.session_state.tickers_input = st.query_params.get(
        "stocks", stocks_to_str(DEFAULT_STOCKS)
    ).split(",")

def update_query_param():
    if st.session_state.tickers_input:
        st.query_params["stocks"] = stocks_to_str(st.session_state.tickers_input)
    else:
        st.query_params.pop("stocks", None)

##Left panel: stock selector
top_left_cell = cols[0].container(
    border=True, height="stretch", vertical_alignment="center"
)
with top_left_cell:
    tickers = st.multiselect(
        "Stock tickers",
        options=sorted(set(SYMBOLS) | set(st.session_state.tickers_input)),
        default=st.session_state.tickers_input,
        placeholder="Choose stocks to compare. Example: NVDA",
        accept_new_options=True,
    )

tickers = [t.upper() for t in tickers]

if tickers:
    st.query_params["stocks"] = stocks_to_str(tickers)
else:
    st.query_params.pop("stocks", None)
if not tickers:
    top_left_cell.info("Pick some stocks to compare", icon=":material/info:")
    st.stop()
##right panel: OHVLC chart