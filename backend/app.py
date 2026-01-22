from datetime import datetime, timedelta
from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import dict_factory
import logging
import time
from typing import Optional, List
import pandas as pd
from zoneinfo import ZoneInfo
from vnstock import Listing


company_list = Listing(source='vci')
cp_list = company_list.symbols_by_group('VN30')
SYMBOLS = list(cp_list)


vn_zone = ZoneInfo("Asia/Ho_Chi_Minh")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ohlvc_api")

app = FastAPI(title="OHVLC Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
CASSANDRA_HOSTS = ["cassandra"]
KEYSPACE = "market"


def get_db():
    session = get_cassandra_session()
    try:
        yield session
    finally:
        session.shutdown()

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
        LIMIT 500
    """
    rows = session.execute(
        query,
        (symbol, interval)
    )
    return list(rows)

# def get_ohlvc_all_symbol(session,symbols,interval= "1m") -> pd.DataFrame:
#     query = """
#         SELECT time, open, high, low, close, volume
#         FROM ohlvc
#         WHERE symbol = %s AND screener = %s
#         ORDER BY time ASC
#     """
#     results = []
#     for symbol in symbols:
#         rows = session.execute(query, (symbol,interval,))
#         results.extend(rows)
#     return results


def get_trading(session,symbols):
    query = """
        SELECT *
        FROM trading_data
        WHERE symbol = %s
        LIMIT 1;
    """
    results = []
    for symbol in symbols:
        rows = session.execute(query, (symbol,))
        results.extend(rows)
    return results

def prediction_data(session,symbol):
    query = """
        SELECT time, time_step, close_predict, screener
        FROM prediction
        WHERE symbol = %s
    """
    rows = session.execute(
        query,
        (symbol,)
    )
    return list(rows)
def tick_news(session,symbol):
    query = """
        SELECT title,link,public_date,s_content,close,price_change_pct
        FROM news
        WHERE symbol = %s
        LIMIT 5
    """
    rows = session.execute(
        query,
        (symbol,)
    )
    return list(rows)



@app.get("/ohlcv/{symbol}")
def get_ohvlc_1_tick(symbol,session = Depends(get_db),interval: str = Query("1m")):
    try:
        return get_ohlvc(session, symbol,interval)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

@app.get("/ohlcv/{symbol}/predict")
def get_ohvlc_1_tick(symbol,session = Depends(get_db)):
    try:
        return prediction_data(session, symbol)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found,f{e}")

@app.get("/tick/{symbol}")
def tick_new(symbol,session = Depends(get_db)):
    try:
        return tick_news(session, symbol)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

# @app.get("/ohlvc/ALL")
# def get_all(session = Depends(get_db) ,symbols = SYMBOLS, interval: str = Query("1m")):
#     try: 
#         return get_ohlvc_all_symbol(session, symbols, interval)
#     except Exception as e:
#         raise HTTPException(status_code=404, detail=f"Symbol not found")

@app.get("/trading")
def get_trade(session = Depends(get_db),symbols = SYMBOLS):
    try:    
        return get_trading(session,symbols)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Symbol not found")


@app.get("/health")
def health_check():
    """
    Health check + Cassandra
    """
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect(KEYSPACE)
        session.execute("SELECT now() FROM system.local")
        session.shutdown()
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/")
def check_date():
    date_now = datetime.now(vn_zone)
    if date_now.weekday() in [5, 6]:
        return {"message": "Market is closed, all data is last avalable from Friday"}
    else:
        return {"message": "Is trade day"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )