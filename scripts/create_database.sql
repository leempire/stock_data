CREATE DATABASE IF NOT EXISTS stock_data;
USE stock_data;

CREATE TABLE stock_daily IF NOT EXISTS (
    ts_code VARCHAR(10) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    open DECIMAL(10, 2) NOT NULL,
    high DECIMAL(10, 2) NOT NULL,
    low DECIMAL(10, 2) NOT NULL,
    close DECIMAL(10, 2) NOT NULL,
    pre_close DECIMAL(10, 2) NOT NULL,
    change DECIMAL(10, 2) NOT NULL,
    pct_chg DECIMAL(10, 2) NOT NULL,
    vol DECIMAL(15, 2) NOT NULL,
    amount DECIMAL(15, 4) NOT NULL,
    PRIMARY KEY (ts_code, trade_date),
    FOREIGN KEY (ts_code) REFERENCES stock_basic(ts_code)
);

CREATE TABLE stock_basic (
    ts_code VARCHAR(10) NOT NULL,
    symbol VARCHAR(10),
    name VARCHAR(50),
    area VARCHAR(20),
    industry VARCHAR(50),
    cnspell VARCHAR(50),
    market VARCHAR(20),
    list_date VARCHAR(8),
    act_name VARCHAR(50),
    act_ent_type VARCHAR(20),
    PRIMARY KEY (ts_code),
);