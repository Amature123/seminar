CREATE KEYSPACE IF NOT EXISTS market
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS market.trading_data (
    handle_time timestamp,
    symbol text,
    ceiling double,
    floor double,
    reference double,
    room_foreign double,
    foreign_buy_volume double,
    foreign_sell_volume double,
    buy_1 double,
    buy_1_volume double,
    buy_2 double,
    buy_2_volume double,
    buy_3 double,
    buy_3_volume double,
    sell_1 double,
    sell_1_volume double,
    sell_2 double,
    sell_2_volume double,
    sell_3 double,
    sell_3_volume double,
    highest double,
    lowest double,
    average double,
    PRIMARY KEY (symbol, handle_time)
)WITH CLUSTERING ORDER BY (handle_time DESC);

CREATE TABLE IF NOT EXISTS market.ohlvc (
    screener text,
    symbol text,
    time timestamp,
    open double,
    high double,
    low double,
    close double,
    volume double,
    PRIMARY KEY ((screener, symbol), time)
)WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE IF NOT EXISTS market.news (
    symbol text,
    id text,
    title text,
    link text,
    public_date timestamp,
    s_content text,
    close double,
    price_change_pct double,
    label text,
    sentiment_score double,
    PRIMARY KEY (symbol, id)
);

CREATE TABLE IF NOT EXISTS market.prediction(
    symbol text,
    screener text,
    time timestamp,
    time_step timestamp,
    close_predict double,
    model text,
    created_at timestamp,
    PRIMARY KEY ((symbol), time, time_step)
)WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE IF NOT EXISTS market.indicators (
    screener text,
    symbol text,
    time timestamp,
    close double,
    sma20 double,
    ema12 double,
    ema26 double,
    macd double,
    rsi14 double,
    bb_mid double,
    bb_upper double,
    bb_lower double,
    vwap double,
    ret double,
    PRIMARY KEY ((screener, symbol), time)
)WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE IF NOT EXISTS market.signals (
    symbol text,
    signal_time timestamp,
    signal_type text,
    side text,
    price double,
    detail text,
    PRIMARY KEY ((symbol), signal_time, signal_type)
)WITH CLUSTERING ORDER BY (signal_time DESC);

CREATE TABLE IF NOT EXISTS market.news_impact (
    symbol text,
    id text,
    news_time timestamp,
    title text,
    link text,
    price_before double,
    price_after double,
    max_price double,
    min_price double,
    n_candles bigint,
    impact_pct double,
    PRIMARY KEY ((symbol), news_time, id)
)WITH CLUSTERING ORDER BY (news_time DESC);