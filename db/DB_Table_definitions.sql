CREATE TABLE IF NOT EXISTS tbl_EquityOptions (
    Date TEXT,
    Type TEXT,
    SubType TEXT,
    Action TEXT,
    Symbol TEXT,
    Instrument_Type TEXT,
    Description TEXT,
    Value REAL,
    Quantity REAL,
    Average_Price REAL,
    Commissions REAL,
    Fees REAL,
    Multiplier INTEGER,
    Root_Symbol TEXT,
    Underlying_Symbol TEXT,
    Expiration_Date TEXT,
    Strike_Price REAL,
    Call_or_Put TEXT,
    Order_Number TEXT,
    Total REAL,
    Currency TEXT,
    isMatched INTEGER DEFAULT 0,
    hash_id TEXT
);

CREATE TABLE IF NOT EXISTS tbl_FuturesOptions (
    Date TEXT,
    Type TEXT,
    SubType TEXT,
    Action TEXT,
    Symbol TEXT,
    Instrument_Type TEXT,
    Description TEXT,
    Value REAL,
    Quantity REAL,
    Average_Price REAL,
    Commissions REAL,
    Fees REAL,
    Multiplier INTEGER,
    Root_Symbol TEXT,
    Underlying_Symbol TEXT,
    Expiration_Date TEXT,
    Strike_Price REAL,
    Call_or_Put TEXT,
    Order_Number TEXT,
    Total REAL,
    Currency TEXT,
    isMatched INTEGER DEFAULT 0,
    hash_id TEXT
);

CREATE TABLE IF NOT EXISTS tbl_Futures (
    Date TEXT,
    Type TEXT,
    SubType TEXT,
    Action TEXT,
    Symbol TEXT,
    Instrument_Type TEXT,
    Description TEXT,
    Value REAL,
    Quantity REAL,
    Average_Price REAL,
    Commissions REAL,
    Fees REAL,
    Multiplier INTEGER,
    Root_Symbol TEXT,
    Underlying_Symbol TEXT,
    Expiration_Date TEXT,
    Strike_Price REAL,
    Call_or_Put TEXT,
    Order_Number TEXT,
    Total REAL,
    Currency TEXT,
    isMatched INTEGER DEFAULT 0,
    hash_id TEXT
);

CREATE TABLE IF NOT EXISTS tbl_Equities (
    Date TEXT,
    Type TEXT,
    SubType TEXT,
    Action TEXT,
    Symbol TEXT,
    Instrument_Type TEXT,
    Description TEXT,
    Value REAL,
    Quantity REAL,
    Average_Price REAL,
    Commissions REAL,
    Fees REAL,
    Multiplier INTEGER,
    Root_Symbol TEXT,
    Underlying_Symbol TEXT,
    Expiration_Date TEXT,
    Strike_Price REAL,
    Call_or_Put TEXT,
    Order_Number TEXT,
    Total REAL,
    Currency TEXT,
    hash_id TEXT,
    isMatched INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tbl_EquityOptions_hash_id ON tbl_EquityOptions(hash_id);
CREATE INDEX IF NOT EXISTS idx_tbl_FuturesOptions_hash_id ON tbl_FuturesOptions(hash_id);
CREATE INDEX IF NOT EXISTS idx_tbl_Futures_hash_id ON tbl_Futures(hash_id);
CREATE INDEX IF NOT EXISTS idx_tbl_Equities_hash_id ON tbl_Equities(hash_id);
CREATE INDEX IF NOT EXISTS idx_tbl_rawdata_hash_id ON tbl_rawdata(hash_id);

CREATE TABLE IF NOT EXISTS tbl_MatchedTrades (symbol TEXT, Description TEXT, openId INTEGER, closeId INTEGER, openDate TEXT, closeDate TEXT, quantity INTEGER, openCost REAL, closeCost REAL, gainPerUnit REAL, netProceeds REAL, openHashId TEXT, closeHashId TEXT)

CREATE UNIQUE INDEX IF NOT EXISTS idx_matched_open_close ON tbl_MatchedTrades(openId, closeId)

CREATE TABLE IF NOT EXISTS tbl_OpenPositions (side TEXT, id INTEGER, symbol TEXT, openDate TEXT, quantity INTEGER, price REAL, hashId TEXT)

CREATE UNIQUE INDEX IF NOT EXISTS idx_open_positions ON tbl_OpenPositions(side, id)

