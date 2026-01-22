import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time
from utils import SYMBOLS
# Cấu hình trang
st.set_page_config(
    page_title="VN30 Market Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Base URL
API_URL = "http://host.docker.internal:8000"  

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        padding: 10px;
        border-radius: 5px;
    }
    .stock-card {
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

# Hàm gọi API
@st.cache_data(ttl=60)
def get_health_check():
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.json()
    except:
        return {"status": "unhealthy", "database": "disconnected"}

@st.cache_data(ttl=30)
def get_ohlcv(symbol, interval="1m"):
    try:
        response = requests.get(f"{API_URL}/ohlcv/{symbol}?interval={interval}", timeout=10)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        return None
    except:
        return None

@st.cache_data(ttl=60)
def get_prediction(symbol):
    try:
        response = requests.get(f"{API_URL}/ohlcv/{symbol}/predict", timeout=10)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        return None
    except:
        return None

@st.cache_data(ttl=30)
def get_trading_data():
    try:
        response = requests.get(f"{API_URL}/trading", timeout=10)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        return None
    except:
        return None

@st.cache_data(ttl=60)
def get_news(symbol):
    try:
        response = requests.get(f"{API_URL}/tick/{symbol}", timeout=10)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

# Header
col1, col2, col3 = st.columns([2, 3, 2])
with col2:
    st.title("📊 VN30 Market Dashboard")

# Kiểm tra kết nối
health = get_health_check()
if health["status"] == "healthy":
    st.success("✅ Kết nối database thành công")
else:
    st.error("❌ Không thể kết nối database")
    st.stop()

# Sidebar
with st.sidebar:
    st.header("⚙️ Cấu hình")
    
    # Danh sách mã chứng khoán VN30 (hardcode hoặc lấy từ API)
    vn30_symbols = SYMBOLS
    
    selected_symbol = st.selectbox("Chọn mã chứng khoán", vn30_symbols)
    
    interval_map = {
        "1 phút": "1m",
        "5 phút": "5m",
        "15 phút": "15m",
        "30 phút": "30m",
        "1 giờ": "1H"
    }
    selected_interval = st.selectbox("Khung thời gian", list(interval_map.keys()))
    
    auto_refresh = st.checkbox("Tự động làm mới", value=False)
    if auto_refresh:
        refresh_interval = st.slider("Làm mới mỗi (giây)", 10, 300, 30)
    
    st.divider()
    st.caption(f"Cập nhật: {datetime.now().strftime('%H:%M:%S')}")


tab1, tab2, tab3, tab4 = st.tabs(["📈 Biểu đồ", "📊 Dữ liệu giao dịch", "🔮 Dự đoán", "📰 Tin tức"])

# Tab 1: Biểu đồ OHLCV
with tab1:
    st.subheader(f"Biểu đồ {selected_symbol}")
    
    df_ohlcv = get_ohlcv(selected_symbol, interval_map[selected_interval])
    
    if df_ohlcv is not None and not df_ohlcv.empty:
        # Chuyển đổi timestamp
        if 'time' in df_ohlcv.columns:
            df_ohlcv['time'] = pd.to_datetime(df_ohlcv['time'])
            df_ohlcv = df_ohlcv.sort_values('time')
        
        # Metrics row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        latest = df_ohlcv.iloc[-1]
        previous = df_ohlcv.iloc[-2] if len(df_ohlcv) > 1 else latest
        
        price_change = latest['close'] - previous['close']
        price_change_pct = (price_change / previous['close']) * 100
        
        with col1:
            st.metric("Giá đóng cửa", f"{latest['close']:,.2f}", 
                     f"{price_change:+,.2f} ({price_change_pct:+.2f}%)")
        with col2:
            st.metric("Cao nhất", f"{latest['high']:,.2f}")
        with col3:
            st.metric("Thấp nhất", f"{latest['low']:,.2f}")
        with col4:
            st.metric("Mở cửa", f"{latest['open']:,.2f}")
        with col5:
            st.metric("Khối lượng", f"{latest['volume']:,.0f}")
        
        # Candlestick chart
        fig = make_subplots(
            rows=2, cols=1,
            row_heights=[0.7, 0.3],
            vertical_spacing=0.05,
            subplot_titles=(f'{selected_symbol} - Giá', 'Khối lượng'),
            shared_xaxes=True
        )
        
        # Candlestick
        fig.add_trace(
            go.Candlestick(
                x=df_ohlcv['time'],
                open=df_ohlcv['open'],
                high=df_ohlcv['high'],
                low=df_ohlcv['low'],
                close=df_ohlcv['close'],
                name='OHLC'
            ),
            row=1, col=1
        )
        
        # Volume bars
        colors = ['red' if df_ohlcv['close'].iloc[i] < df_ohlcv['open'].iloc[i] 
                 else 'green' for i in range(len(df_ohlcv))]
        
        fig.add_trace(
            go.Bar(
                x=df_ohlcv['time'],
                y=df_ohlcv['volume'],
                name='Khối lượng',
                marker_color=colors,
                showlegend=False
            ),
            row=2, col=1
        )
        
        fig.update_layout(
            height=700,
            xaxis_rangeslider_visible=False,
            hovermode='x unified',
            template='plotly_white'
        )
        
        fig.update_xaxes(title_text="Thời gian", row=2, col=1)
        fig.update_yaxes(title_text="Giá (VND)", row=1, col=1)
        fig.update_yaxes(title_text="Khối lượng", row=2, col=1)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Hiển thị dữ liệu dạng bảng
        with st.expander("📋 Xem dữ liệu chi tiết"):
            st.dataframe(df_ohlcv.sort_values('time', ascending=False), use_container_width=True)
    else:
        st.warning("Không có dữ liệu OHLCV")

# Tab 2: Trading Data
with tab2:
    st.subheader("Dữ liệu giao dịch thời gian thực")
    
    df_trading = get_trading_data()
    
    if df_trading is not None and not df_trading.empty:
        # Tìm dữ liệu của symbol được chọn
        symbol_data = df_trading[df_trading['symbol'] == selected_symbol]
        
        if not symbol_data.empty:
            data = symbol_data.iloc[0]
            
            # Display key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Tham chiếu", f"{data.get('reference', 0):,.2f}")
                st.metric("Trần", f"{data.get('ceiling', 0):,.2f}")
                st.metric("Sàn", f"{data.get('floor', 0):,.2f}")
            
            with col2:
                st.metric("Cao nhất", f"{data.get('highest', 0):,.2f}")
                st.metric("Thấp nhất", f"{data.get('lowest', 0):,.2f}")
                st.metric("Trung bình", f"{data.get('average', 0):,.2f}")
            
            with col3:
                st.metric("NN Mua", f"{data.get('foreign_buy_volume', 0):,.0f}")
                st.metric("NN Bán", f"{data.get('foreign_sell_volume', 0):,.0f}")
                net_foreign = data.get('foreign_buy_volume', 0) - data.get('foreign_sell_volume', 0)
                st.metric("NN Ròng", f"{net_foreign:+,.0f}")
            
            with col4:
                st.metric("Room NN", f"{data.get('room_foreign', 0):,.0f}")
            
            st.divider()
            
            # Bảng giá mua/bán
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🟢 Giá Mua")
                buy_data = {
                    "Giá": [
                        data.get('buy_3', 0),
                        data.get('buy_2', 0),
                        data.get('buy_1', 0)
                    ],
                    "Khối lượng": [
                        data.get('buy_3_volume', 0),
                        data.get('buy_2_volume', 0),
                        data.get('buy_1_volume', 0)
                    ]
                }
                st.dataframe(pd.DataFrame(buy_data), use_container_width=True)
            
            with col2:
                st.markdown("### 🔴 Giá Bán")
                sell_data = {
                    "Giá": [
                        data.get('sell_1', 0),
                        data.get('sell_2', 0),
                        data.get('sell_3', 0)
                    ],
                    "Khối lượng": [
                        data.get('sell_1_volume', 0),
                        data.get('sell_2_volume', 0),
                        data.get('sell_3_volume', 0)
                    ]
                }
                st.dataframe(pd.DataFrame(sell_data), use_container_width=True)
        
        # Hiển thị toàn bộ VN30
        with st.expander("📊 Xem toàn bộ VN30"):
            st.dataframe(df_trading, use_container_width=True)
    else:
        st.warning("Không có dữ liệu giao dịch")

# Tab 3: Prediction
with tab3:
    st.subheader(f"Dự đoán giá {selected_symbol}")
    
    df_pred = get_prediction(selected_symbol)
    
    if df_pred is not None and not df_pred.empty:
        # Convert timestamp
        if 'time' in df_pred.columns:
            df_pred['time'] = pd.to_datetime(df_pred['time'])
            df_pred['time_step'] = pd.to_datetime(df_pred['time_step'])
        
        # Lấy danh sách screener (interval) có trong data
        available_screeners = sorted(df_pred['screener'].unique())
        
        # Thêm selectbox để chọn screener/interval
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_screener = st.selectbox(
                "Chọn khung thời gian dự đoán:",
                options=available_screeners,
                index=0
            )
        
        with col2:
            st.metric("Số điểm dữ liệu", len(df_pred[df_pred['screener'] == selected_screener]))
        
        # Lọc data theo screener đã chọn
        df_filtered = df_pred[df_pred['screener'] == selected_screener].copy()
        df_filtered = df_filtered.sort_values('time_step')
        
        # Chart dự đoán
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df_filtered['time_step'],
            y=df_filtered['close_predict'],
            mode='lines+markers',
            name='Dự đoán',
            line=dict(color='orange', width=2),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title=f"Dự đoán giá {selected_symbol} - Khung {selected_screener}",
            xaxis_title="Thời gian",
            yaxis_title="Giá dự đoán (VND)",
            height=500,
            hovermode='x unified',
            template='plotly_white'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display prediction table
        display_df = df_filtered[['time', 'time_step', 'close_predict', 'screener']].copy()
        display_df.columns = ['Thời điểm dự đoán', 'Thời gian dự báo', 'Giá dự đoán', 'Khung thời gian']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("Chưa có dữ liệu dự đoán cho mã này")
# Tab 4: News
with tab4:
    st.subheader(f"Tin tức {selected_symbol}")
    
    news_data = get_news(selected_symbol)
    
    if news_data:
        for item in news_data:
            with st.container():
                st.markdown(f"### [{item.get('title', 'N/A')}]({item.get('link', '#')})")
                st.caption(f"Ngày: {item.get('public_date', 'N/A')}")
                st.write(item.get('s_content', 'Không có nội dung'))
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Giá đóng cửa", f"{item.get('close',0):,.2f}")
                with col2:
                    st.metric("Thay đổi %", f"{item.get('price_change_pct',0):+.2f}%")
                
                st.divider()
    else:
        st.info("Không có tin tức")

# Auto refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()