const db = require('./connection');
const fs = require('fs');

function isBuyOpen(trade) { return trade.Action === 'BUY_TO_OPEN'; }
function isSellClose(trade) { return trade.Action === 'SELL_TO_CLOSE'; }
function isSellOpen(trade) { return trade.Action === 'SELL_TO_OPEN'; }
function isBuyClose(trade) { return trade.Action === 'BUY_TO_CLOSE'; }

function runMatch() {
  db.all(`SELECT rowid AS id, Date, Symbol, Expiration_Date AS expDate, Strike_Price AS strike, [Call_or_Put] AS type, Action, Quantity AS qty, Average_Price AS price, Underlying_Symbol as rootSymbol, round((Commissions + Fees)/Quantity,2) AS costPerUnit, hash_id FROM tbl_FuturesOptions WHERE isMatched != 1 ORDER BY Date ASC`, (err, rows) => {
    if (err) throw err;
    const openLongs = {};
    const openShorts = {};
    const matches = [];
    for (const t of rows) {
      let qty = t.qty;
      const key = `${t.Symbol}|${t.expDate}|${t.strike}|${t.type}`;
      console.log(key);
      if (isBuyOpen(t)) {
        if (!openLongs[key]) openLongs[key] = [];
        openLongs[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, rootSymbol: t.rootSymbol, costPerUnit: t.costPerUnit, openHashId: t.hash_id });
        
      } else if (isSellClose(t)) {
        const queue = openLongs[key] || [];
        while (qty > 0 && queue.length > 0) {
          const open = queue[0];
          const m = Math.min(open.qty, qty);
          matches.push({ symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, openId: open.id, closeId: t.id, quantity: m, openDate: open.date, closeDate: t.Date, openPrice: open.price, closePrice: t.price, rootSymbol: t.rootSymbol, costPerUnit: t.costPerUnit + open.costPerUnit, openHashId: open.openHashId, closeHashId: t.hash_id });
          open.qty -= m;
          qty -= m;
          if (open.qty === 0) queue.shift();
        }
        if (qty > 0) {
          // unmatched close; treat as short open
          if (!openShorts[key]) openShorts[key] = [];
          openShorts[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, rootSymbol: t.rootSymbol, costPerUnit: t.costPerUnit, closeHashId: t.hash_id });
        }
      } else if (isSellOpen(t)) {
        if (!openShorts[key]) openShorts[key] = [];
        openShorts[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, rootSymbol: t.rootSymbol, costPerUnit: t.costPerUnit, openHashId: t.hash_id });
      } else if (isBuyClose(t)) {
        const queue = openShorts[key] || [];
        while (qty > 0 && queue.length > 0) {
          const open = queue[0];
          const m = Math.min(open.qty, qty);
          matches.push({ symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, openId: open.id, closeId: t.id, quantity: m, openDate: open.date, closeDate: t.Date, openPrice: open.price, closePrice: t.price, rootSymbol: t.rootSymbol, costPerUnit: t.costPerUnit + open.costPerUnit, openHashId: open.openHashId, closeHashId: t.hash_id });
          open.qty -= m;
          qty -= m;
          if (open.qty === 0) queue.shift();
        }
        if (qty > 0) {
          // unmatched close to long
          if (!openLongs[key]) openLongs[key] = [];
          openLongs[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, rootSymbol: t.rootSymbol, costPerUnit: t.costPerUnit, openHashId: t.hash_id });
        }
      }
    }
    // flatten queues
    const unmatchedLongs = Object.values(openLongs).flat();
    const unmatchedShorts = Object.values(openShorts).flat();
    console.log(`Options matched: ${matches.length}`);
    // export to CSV - Commented out for now.
    // exportCsv(matches, unmatchedLongs, unmatchedShorts);
    writeToDb(matches, unmatchedLongs, unmatchedShorts);
    // Do not close the db here; connection is shared globally.
  });
}

function exportCsv(matches, longs, shorts) {
  const hdrM = ['rootSymbol,symbol,expDate,strike,type,openId,closeId,quantity,openDate,closeDate,openPrice,closePrice,gainPerUnit,netProceeds,openHashId,closeHashId'];
  matches.forEach(m => hdrM.push([m.rootSymbol, m.symbol, m.expDate, m.strike, m.type, m.openId, m.closeId, m.quantity, m.openDate, m.closeDate, m.openPrice, m.closePrice, m.closePrice - m.openPrice, ((m.closePrice - m.openPrice + m.costPerUnit) * m.quantity).toFixed(2), m.openHashId, m.closeHashId].join(',')));
  fs.writeFileSync('matched_options.csv', hdrM.join('\n'));
  console.log('Wrote matched_options.csv');
  const hdrU = ['side,id,symbol,expDate,strike,type,quantity,date,price,costPerUnit,openHashId,closeHashId'];
  longs.forEach(o => hdrU.push(['buy_open', o.id, o.Symbol, o.expDate, o.strike, o.type, o.qty, o.date, o.price, o.costPerUnit, o.openHashId, o.closeHashId].join(',')));
  shorts.forEach(o => hdrU.push(['sell_open', o.id, o.Symbol, o.expDate, o.strike, o.type, o.qty, o.date, o.price, o.costPerUnit, o.openHashId, o.closeHashId].join(',')));
  fs.writeFileSync('unmatched_options.csv', hdrU.join('\n'));
  console.log('Wrote unmatched_options.csv');
}

/**
 * Persist matched and unmatched trades to DB tables and update isMatched
 */
function writeToDb(matches, openLongs, openShorts) {
  db.serialize(() => {
      /* Only needed first time when DB is created.*/
      //-------------------------------------------
      db.run("CREATE TABLE IF NOT EXISTS tbl_MatchedOptionTrades ( rootSymbol TEXT, symbol TEXT, ExpDate TEXT, strike TEXT, type TEXT, openId INTEGER, closeId INTEGER, openDate TEXT, closeDate TEXT, quantity INTEGER, costPerUnit REAL, gainPerUnit REAL, netProceeds REAL, openHashId TEXT, closeHashId TEXT)");

      db.run("CREATE TABLE IF NOT EXISTS tbl_OpenOptionPositions (side TEXT, rootSymbol TEXT, symbol TEXT, ExpDate TEXT, strike TEXT, type TEXT, openDate TEXT, quantity INTEGER, price REAL, costPerUnit REAL, openHashId TEXT, closeHashId TEXT)");
      

      // ensure no duplicate matched entries
      db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_matched_open_close ON tbl_MatchedOptionTrades(openDate, closeDate, symbol, quantity)");
      // ensure no duplicate open positions
      db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_open_positions ON tbl_OpenOptionPositions(side, symbol)");
      
      const mt = db.prepare("INSERT OR IGNORE INTO tbl_MatchedOptionTrades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      matches.forEach(m => {
          const gainPerUnit = m.closePrice - m.openPrice;
          const netProceeds = ((gainPerUnit+ m.costPerUnit) * m.quantity).toFixed(2);
          mt.run(
              m.rootSymbol, m.symbol, m.expDate, m.strike, m.type, m.openId, m.closeId, m.openDate, m.closeDate, m.quantity, m.costPerUnit, gainPerUnit, netProceeds, m.openHashId, m.closeHashId,
              (err) => {
                  // ignore unique constraint failures
                  if (err && err.code !== 'SQLITE_CONSTRAINT') console.error(err);
              }
          );
      });
      mt.finalize();
      const op = db.prepare("INSERT INTO tbl_OpenOptionPositions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
      openLongs.forEach(o => op.run('buy_open', o.rootSymbol, o.Symbol, o.expDate, o.strike, o.type, o.openDate, o.qty, o.price, o.costPerUnit, o.openHashId, o.closeHashId));
      openShorts.forEach(o => op.run('sell_open', o.rootSymbol, o.Symbol, o.expDate, o.strike, o.type, o.openDate, o.qty, o.price, o.costPerUnit, o.openHashId, o.closeHashId));
      op.finalize();

      matches.forEach(m => {
          db.run("UPDATE tbl_FuturesOptions SET isMatched = 1 WHERE rowid IN (?,?)", m.openId, m.closeId);
      });
  });
}



runMatch();
