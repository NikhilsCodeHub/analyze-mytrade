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
delete from tbl_tempData;
delete from tbl_rawdata;
*/

INSERT INTO tbl_finaltrades (Date,Type,SubType,Action,Symbol,InstrumentType,Description,Value,Quantity,AveragePrice,Commissions,Fees,Multiplier,RootSymbol,UnderlyingSymbol,ExpirationDate,StrikePrice,CallorPut,OrderNumber,Total,Currency) 


SELECT Date,Type,Sub-Type,Action,Symbol,InstrumentType,Description,Value,Quantity,AveragePrice,Commissions,Fees,Multiplier,RootSymbol,UnderlyingSymbol,ExpirationDate,StrikePrice,CallorPut,OrderNumber,Total,Currency FROM tbl_tempData ;

Insert INTO tbl_rawdata
(Date,Type,Sub_Type,Action,Symbol,Instrument_Type,Description,Value,Quantity,Average_Price,Commissions,Fees,Multiplier,Root_Symbol,Underlying_Symbol,Expiration_Date,Strike_Price,Call_or_Put,Order_Number,Total,Currency,hash_id)
SELECT Date,Type,Sub_Type,Action,Symbol,Instrument_Type,Description,Value,Quantity,Average_Price,Commissions,Fees,Multiplier,Root_Symbol,Underlying_Symbol,Expiration_Date,Strike_Price,Call_or_Put,Order_Number,Total,Currency,hash_id FROM  tbl_tempData
where hash_id NOT IN (SELECT hash_id FROM tbl_rawdata);

SELECT Date, Action, Description ,Quantity,Total, Order_Number
FROM tbl_rawdata
Where Instrument_Type = 'Future' and Date BETWEEN '2025-04-16' AND '2025-04-19'
Order by Date ASC;

SELECT Date, Symbol, Action, Description ,Quantity,Total, Order_Number, isMatched
FROM tbl_Futures
Where Date BETWEEN '2025-04-09' AND '2025-04-10'
Order by Date ASC;

SELECT rowid AS id, Date, Description, Symbol, Expiration_Date AS expDate, Action, Quantity AS qty, Average_Price AS price, round((Commissions + Fees)/Quantity,2) AS costPerUnit, round(Total/Quantity,2) AS totalPerUnit, hash_id FROM tbl_Futures WHERE isMatched != 1 ORDER BY Date ASC
/* 
Select count(*)
From tbl_Futures;

Select sum(netProceeds)
From tbl_MatchedTrades
where Symbol = '/ESM5'
order by OpenDate ASC;



Update tbl_Futures
Set isMatched = 0
where description LIKE '%mark to market%';


select 130.54 + 2877.3

Select sum(Total) from tbl_FuturesOptions
where Symbol like './ESM5%'
// where date > '2025-04-09T15:00:00-0500';

Update tbl_Futures
Set isMatched = 0;
Delete from tbl_MatchedTrades;


where description LIKE '%mark to market%';

Delete
FROM tbl_Futures
Where Date BETWEEN '2025-04-16' AND '2025-04-19' */

-- WHERE hash_id NOT IN (SELECT hash_id FROM tbl_rawdata);

Date,Type,Sub_Type,Action,Symbol,Instrument_Type,Description,Value,Quantity,Average_Price,Commissions,Fees,Multiplier,Root_Symbol,Underlying_Symbol,Expiration_Date,Strike_Price,Call_or_Put,Order_Number,Total,Currency,hash_id

SELECT count(*) 
FROM tbl_rawdata 
WHERE "Instrument_Type" = 'Future'
Group by hash_id, Date
having count(*) > 1
EXCEPT
SELECT hash_id FROM tbl_EquityOptions


SELECT rowid AS id, Date, Symbol, Expiration_Date AS expDate, Action, Quantity AS qty, Average_Price AS price, round((Commissions + Fees)/Quantity,2) AS costPerUnit, round(Total/Quantity,2) AS totalPerUnit 
FROM tbl_Futures 
Where Symbol = '/MESZ4'
ORDER BY Date ASC

Select * 
FROM tbl_Futures 
Where Symbol = '/MESZ4'
ORDER BY Date ASC;


SELECT symbol, sum(netProceeds) as RealizedGain
FROM tbl_MatchedTrades
where openDate > '2025-01-01'
GROUP by symbol;

SELECT rowid AS id, Date, Symbol, Expiration_Date AS expDate, Strike_Price AS strike, [Call_or_Put] AS type, Action, Quantity AS qty, Average_Price AS price FROM tbl_FuturesOptions ORDER BY Date ASC