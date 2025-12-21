# api/main.py
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
SYMBOLS =["ACB","BCM","BID","CTG","DGC","FPT","GAS","GVR","HDB","HPG",
"TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE"]

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

def get_latest_all_symbols(
    session,
    symbols: List[str],
    limit_per_symbol: int = 1
):
    results = []
    for symbol in symbols:
        query = """
            SELECT symbol, time, open, high, low, close, volume
            FROM OHVLC
            WHERE symbol = %s
            LIMIT %s
        """
        rows = session.execute(query, (symbol, limit_per_symbol))
        results.extend(rows)
    return results

def get_latest_trading_all_symbols(
    session,
    symbols: List[str],
    limit_per_symbol: int = 1
):
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


@app.get("/ohlvc/summary")
def ohlvc_summary(
    session = Depends(get_db)
):
    return get_latest_all_symbols(session, SYMBOLS, limit_per_symbol=1)

@app.get("/trading/summary")
def tradig_summary(
    session = Depends(get_db)
):
    return get_latest_trading_all_symbols(session, SYMBOLS, limit_per_symbol=1)

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
