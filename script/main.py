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
from lightweight_charts.widgets import StreamlitChart,AbstractChart
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

def get_ohlvc(session, symbol, interval= "1m") -> pd.DataFrame:
    query = """
        SELECT time, open, high, low, close, volume
        FROM ohlvc
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

def call_1_tick(session, symbol, interval= "1m") -> pd.DataFrame:
    query = """
        SELECT time, open, high, low, close, volume
        FROM ohlvc
        WHERE symbol = %s AND screener = %s
        LIMIT 1
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

def get_latest_trading_all_symbols(session,symbols,limit_per_symbol= 1):
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
st.markdown("""
# :material/query_stats: Stock peer analysis
Easily compare stocks against others in their peer group.
""")

option = st.selectbox(
    "Please select a symbol",
    SYMBOLS,
    key="selected_symbol"
)
session = get_cassandra_session()

@st.fragment(run_every="200ms")
def realtime_tick():
    symbol = st.session_state.selected_symbol
    df_key = f"ohlvc_df_{symbol}"

    tick_df = call_1_tick(session, symbol)
    if tick_df.empty:
        return
    df = st.session_state[df_key]
    df = pd.concat([df, tick_df])
    df = df.drop_duplicates(subset="time", keep="last")
    df = df.sort_values("time")

    st.session_state[df_key] = df
    chart = st.session_state.chart
    chart.update(tick_df.iloc[-1])

    st.session_state.sma_line.set(calculate_sma(df, 50))
    st.session_state.ema_line.set(calculate_ema(df, 50))

df_key = f"ohlvc_df_{option}"
if df_key not in st.session_state:
    with st.spinner("Loading OHLCV history..."):
        df = get_ohlvc(session, option)
        df = df.sort_values("time")
        st.session_state[df_key] = df

if "chart" not in st.session_state:
    df = st.session_state[df_key]

    chart = StreamlitChart(width=900, height=600)
    chart.set(df,keep_drawings=True)

    sma_line = chart.create_line("SMA 50")
    ema_line = chart.create_line("EMA 50")

    sma_line.set(calculate_sma(df, 50))
    ema_line.set(calculate_ema(df, 50))

    chart.load()

    st.session_state.chart = chart
    st.session_state.sma_line = sma_line
    st.session_state.ema_line = ema_line
    realtime_tick()
st.subheader(f"{option} price chart")




st.subheader("Latest trading data")

trading_data = get_latest_trading_all_symbols(
    session,
    SYMBOLS,
    limit_per_symbol=1
)

trading_df = pd.DataFrame(trading_data)
st.dataframe(trading_df, use_container_width=True)
