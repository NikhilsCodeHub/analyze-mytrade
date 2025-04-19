-- SQLite
/*
node .\import_trades.js .\tastytrade_transactions_history_x5WX89994_230101_to_231231.csv, .\tastytrade_transactions_history_x5WX89994_240101_to_241231.csv 

*/

SELECT Date, Type, `Sub Type`, `Action`, Symbol, `Instrument Type`, Description, Value, Quantity, `Average Price`, Commissions, Fees, Multiplier, `Root Symbol`, `Underlying Symbol`, `Expiration Date`, `Strike Price`, `Call or Put`, `Order #`, Total, Currency
FROM tbl_rawdata
where [Order #] IS NOT NULL
Limit 10;


select * FROM tbl_rawdata where [instrument type] = 'Future'
EXCEPT
select * FROM tbl_Futures;

select * FROM tbl_rawdata where [instrument type] = 'Future Option'
EXCEPT
select * FROM tbl_FuturesOptions;

/*
delete from tbl_Equities;
delete from tbl_EquityOptions;
delete from tbl_Futures;
delete from tbl_FuturesOptions;
delete from tbl_rawdata;
*/


select DISTINCT count(*) FROM tbl_EquityOptions;
select count(*) FROM tbl_EquityOptions;

SELECT count(*) from tbl_Equities;
SELECT count(*) from tbl_EquityOptions;
SELECT count(*) from tbl_Futures;
SELECT count(*) from tbl_FuturesOptions;
SELECT count(*) from tbl_rawdata; --where Date BETWEEN '2024-02-01' AND '2024-3-31';


CREATE VIRTUAL TABLE tbl_Virtual USING csv('tastytrade_transactions_history_x5WX89994_230101_to_231231.csv');
Select count(*) from tbl_Virtual;


select  Date,Description,Value,Total count(*) from [2023] 
group by Date,Description,Value,Total
having count(*) > 1;

select  DISTINCT *, count(*) from [2023] 
group by Date,Type,"Sub Type",Action,Symbol,"Instrument Type",Description,Value,Quantity,"Average Price",Commissions,Fees,Multiplier,"Root Symbol","Underlying Symbol","Expiration Date","Strike Price","Call or Put","Order #",Total,Currency
having count(*) > 1;

/*
Columns

Date,Type,"Sub Type",Action,Symbol,"Instrument Type",Description,Value,Quantity,"Average Price",Commissions,Fees,Multiplier,"Root Symbol","Underlying Symbol","Expiration Date","Strike Price","Call or Put","Order #",Total,Currency


*/


CREATE TABLE IF NOT EXISTS tbl_finaltrades (Date TEXT, Type TEXT, Sub-Type TEXT, Action TEXT, Symbol TEXT, Instrument-Type TEXT, Description TEXT, Value REAL, Quantity INTEGER, Average-Price REAL, Commissions REAL, Fees REAL, Multiplier REAL, Root-Symbol TEXT, Underlying-Symbol TEXT, Expiration-Date TEXT, Strike-Price REAL, Call-or-Put TEXT, Order-Number TEXT, Total REAL, Currency TEXT);


select count(*) from tbl_finaltrades;
select count(*) from temp_import;

/*
delete from tbl_finaltrades;
*/

INSERT INTO tbl_finaltrades (Date,Type,SubType,Action,Symbol,InstrumentType,Description,Value,Quantity,AveragePrice,Commissions,Fees,Multiplier,RootSymbol,UnderlyingSymbol,ExpirationDate,StrikePrice,CallorPut,OrderNumber,Total,Currency) 


SELECT Date,Type,SubType,Action,Symbol,InstrumentType,Description,Value,Quantity,AveragePrice,Commissions,Fees,Multiplier,RootSymbol,UnderlyingSymbol,ExpirationDate,StrikePrice,CallorPut,OrderNumber,Total,Currency FROM temp_import ;

SELECT Date,Type,SubType,Action,Symbol,InstrumentType,Description,Value,Quantity,AveragePrice,Commissions,Fees,Multiplier,RootSymbol,UnderlyingSymbol,ExpirationDate,StrikePrice,CallorPut,OrderNumber,Total,Currency FROM tbl_finaltrades;