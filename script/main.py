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
import pandas_ta as ta
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

def get_old_ohlvc (session, symbol: str, interval: str = "1m") -> pd.DataFrame:
    query = """
        SELECT screener, symbol, time, open, high, low, close, volume
        FROM ohlvc_old
        WHERE symbol = %s AND screener = %s
        ALLOW FILTERING
    """
    rows = session.execute(
        query,
        (symbol, interval)
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"])
    return df
def get_latest_a_symbols(session, symbol, interval: str = "1m", limit_per_symbol: int = 1) -> pd.DataFrame:
    query = """
        SELECT symbol, time, open, high, low, close, volume
        FROM ohlvc
            WHERE symbol = %s AND screener = %s
            LIMIT %s
            ALLOW FILTERING
        """
    rows = session.execute(query, (symbol,interval,limit_per_symbol))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"])
    return df

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
        rows = session.execute(query, (symbol, limit_per_symbol,))
        results.extend(rows)
    return results
def stocks_to_str(stocks):
    return ",".join(stocks)

def calculate_sma(df, period: int = 50):
    return pd.DataFrame({
        'time': df['time'],
        f'SMA {period}': df.ta.sma(length=period)
    }).dropna()

def calculate_ema(df, period: int = 50):
    return pd.DataFrame({
        'time': df['time'],
        f'EMA {period}': df.ta.ema(df.ta.ohlc4())
    }).dropna()

#############################################################################################
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

option = st.selectbox(
    "Please select a symbol",
    SYMBOLS,
    key="selected_symbol"
)
session = get_cassandra_session()

@st.fragment(run_every="500ms")
def update_state():
    symbol = st.session_state.selected_symbol
    state_key = f"ohlvc_df_{symbol}"
    state_key_2 = f"ohlvc_udpate_{symbol}"
    st.session_state[state_key] = get_old_ohlvc(session, symbol)
    st.session_state[state_key_2] = get_latest_a_symbols(session, symbol)

update_state()

df = st.session_state.get(f"ohlvc_df_{option}", pd.DataFrame())
df_update = st.session_state.get(f"ohlvc_udpate_{option}",pd.DataFrame())

st.subheader(f"{option} price chart")

if df.empty:
    st.warning("No data")
else:
    chart = StreamlitChart(width=900, height=600)
    chart.set(df)
    sma_line = chart.create_line('SMA 50')
    sma_data = calculate_sma(df, period=50)
    ema_line = chart.create_line('EMA 50')
    ema_data = calculate_ema(df, period=50)
    sma_line.set(sma_data)
    ema_line.set(ema_data)
    for i, series in df_update.iterrows():
        chart.update(series)
        time.sleep(0.1)
    chart.load()

trading_data = get_latest_trading_all_symbols(session, SYMBOLS, limit_per_symbol=1)
trading_df = pd.DataFrame(trading_data)
st.subheader("Latest trading data")
st.dataframe(trading_df)



# if "tickers_input" not in st.session_state:
#     st.session_state.tickers_input = st.query_params.get(
#         "stocks", stocks_to_str(DEFAULT_STOCKS)
#     ).split(",")

# def update_query_param():
#     if st.session_state.tickers_input:
#         st.query_params["stocks"] = stocks_to_str(st.session_state.tickers_input)
#     else:
#         st.query_params.pop("stocks", None)

# ##Left panel: stock selector
# top_left_cell = cols[0].container(
#     border=True, height="stretch", vertical_alignment="center"
# )
# with top_left_cell:
#     tickers = st.multiselect(
#         "Stock tickers",
#         options=sorted(set(SYMBOLS) | set(st.session_state.tickers_input)),
#         default=st.session_state.tickers_input,
#         placeholder="Choose stocks to compare. Example: NVDA",
#         accept_new_options=True,
#     )

# tickers = [t.upper() for t in tickers]

# if tickers:
#     st.query_params["stocks"] = stocks_to_str(tickers)
# else:
#     st.query_params.pop("stocks", None)
# if not tickers:
#     top_left_cell.info("Pick some stocks to compare", icon=":material/info:")
#     st.stop()
##right panel: OHVLC chart