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
import schedule
import utils

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

def get_ohlvc(session, symbol, interval= "1m") -> pd.DataFrame:
    query = """
        SELECT time, open, high, low, close, volume
        FROM ohlvc
        WHERE symbol = %s AND screener = %s
        ORDER BY time ASC
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

# def prediction_data(session,symbol):
#     query = """
#         SELECT time, time_step, close_predict
#         FROM ohlvc
#         WHERE symbol = %s AND screener = %s
#         ORDER BY time ASC
#     """
#     rows = session.execute(
#         query,
#         (symbol,)
#     )
#     df = pd.DataFrame(rows)
#     if df.empty:
#         return df
#     df["time"] = pd.to_datetime(df["time"])
#     return df

def stocks_to_str(stocks):
    return ",".join(stocks)

def calculate_sma(df, period: int = 20):
    return pd.DataFrame({
        'time': df['time'],
        f'SMA {period}': df.ta.sma(length=period)
    }).dropna()

def calculate_ema(df, period: int = 20):
    return pd.DataFrame({
        'time': df['time'],
        f'EMA {period}': df.ta.ema(df.ta.ohlc4())
    }).dropna()

def calculate_metrics(data):
    last_close = data['close'].iloc[-1]
    prev_close = data['close'].iloc[0]
    change = last_close - prev_close
    pct_change = (change / prev_close) * 100
    return last_close, change, pct_change


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
session = get_cassandra_session()
option = st.selectbox(
    "Please select a symbol",
    SYMBOLS,
    key="selected_symbol"
)


st.subheader(f"{option} informations:")
company_info=utils.extract_company_profile(option)
st.markdown(company_info)

st.subheader(f"{option} price info:")
symbol = st.session_state.selected_symbol
state_key = f"ohlvc_df_{symbol}"
st.session_state[state_key] = get_ohlvc(session, symbol)

@st.fragment(run_every="30s")
def update_state():
    df = st.session_state.get(f"ohlvc_df_{option}", pd.DataFrame())
    last_close, change, pct_change = calculate_metrics(df)
    st.metric(label=f"{option} Last Price", value=f"{last_close:.2f} USD", delta=f"{change:.2f} ({pct_change:.2f}%)")
    col1, col2, col3 = st.columns(3)
    col1.metric("High", f"{df['high'].iloc[-1]} VND")
    col2.metric("Low", f"{df['low'].iloc[-1]} VND")
    col3.metric("Volume", f"{df['volume'].iloc[-1]}")

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
        chart.load()


    symbol = st.session_state.selected_symbol
    state_key = f"ohlvc_df_{symbol}"
    st.session_state[state_key] = get_ohlvc(session, symbol)
    df = st.session_state.get(f"ohlvc_df_{option}", pd.DataFrame())

update_state()

@st.fragment(run_every="5s")
def update_trading():
    trading_data = get_latest_trading_all_symbols(
        session,
        SYMBOLS,
        limit_per_symbol=1
    )
    trading_df = pd.DataFrame(trading_data)
    st.dataframe(trading_df, use_container_width=True)
update_trading()