CREATE KEYSPACE IF NOT EXISTS market
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS market.trading_data (
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
    average double,
    PRIMARY KEY (symbol, time)
);

CREATE TABLE IF NOT EXISTS market.OHVLC (
    symbol text,
    time timestamp,
    open double,
    high double,
    low double,
    close double,
    volume double,
    PRIMARY KEY (symbol, time)
);

CREATE TABLE IF NOT EXISTS market.news (
    symbol text,
    id text,
    title text,
    publish_date timestamp,
    PRIMARY KEY (symbol, id)
);
