const db = require('./connection');
const fs = require('fs');

function isBuyOpen(trade) { return trade.Action === 'Buy_To_Open'; }
function isSellClose(trade) { return trade.Action === 'Sell_To_Close'; }
function isSellOpen(trade) { return trade.Action === 'Sell_To_Open'; }
function isBuyClose(trade) { return trade.Action === 'Buy_To_Close'; }

function runMatch() {
  db.all(`SELECT rowid AS id, Date, Symbol, Expiration_Date AS expDate, Strike_Price AS strike, [Call_or_Put] AS type, Action, Quantity AS qty, Average_Price AS price FROM tbl_FuturesOptions ORDER BY Date ASC`, (err, rows) => {
    if (err) throw err;
    const openLongs = {};
    const openShorts = {};
    const matches = [];
    for (const t of rows) {
      let qty = t.qty;
      const key = `${t.Symbol}|${t.expDate}|${t.strike}|${t.type}`;
      if (isBuyOpen(t)) {
        if (!openLongs[key]) openLongs[key] = [];
        openLongs[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type });
        
      } else if (isSellClose(t)) {
        const queue = openLongs[key] || [];
        while (qty > 0 && queue.length > 0) {
          const open = queue[0];
          const m = Math.min(open.qty, qty);
          matches.push({ symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, openId: open.id, closeId: t.id, quantity: m, openDate: open.date, closeDate: t.Date, openPrice: open.price, closePrice: t.price });
          open.qty -= m;
          qty -= m;
          if (open.qty === 0) queue.shift();
        }
        if (qty > 0) {
          // unmatched close; treat as short open
          if (!openShorts[key]) openShorts[key] = [];
          openShorts[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type });
        }
      } else if (isSellOpen(t)) {
        if (!openShorts[key]) openShorts[key] = [];
        openShorts[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type });
      } else if (isBuyClose(t)) {
        const queue = openShorts[key] || [];
        while (qty > 0 && queue.length > 0) {
          const open = queue[0];
          const m = Math.min(open.qty, qty);
          matches.push({ symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type, openId: open.id, closeId: t.id, quantity: m, openDate: open.date, closeDate: t.Date, openPrice: open.price, closePrice: t.price });
          open.qty -= m;
          qty -= m;
          if (open.qty === 0) queue.shift();
        }
        if (qty > 0) {
          // unmatched close to long
          if (!openLongs[key]) openLongs[key] = [];
          openLongs[key].push({ id: t.id, qty, date: t.Date, price: t.price, Symbol: t.Symbol, expDate: t.expDate, strike: t.strike, type: t.type });
        }
      }
    }
    // flatten queues
    const unmatchedLongs = Object.values(openLongs).flat();
    const unmatchedShorts = Object.values(openShorts).flat();
    console.log(`Options matched: ${matches.length}`);
    exportCsv(matches, unmatchedLongs, unmatchedShorts);
    // Do not close the db here; connection is shared globally.
  });
}

function exportCsv(matches, longs, shorts) {
  const hdrM = ['symbol,expDate,strike,type,openId,closeId,quantity,openDate,closeDate,openPrice,closePrice'];
  matches.forEach(m => hdrM.push([m.symbol, m.expDate, m.strike, m.type, m.openId, m.closeId, m.quantity, m.openDate, m.closeDate, m.openPrice, m.closePrice].join(',')));
  fs.writeFileSync('matched_options.csv', hdrM.join('\n'));
  console.log('Wrote matched_options.csv');
  const hdrU = ['side,id,symbol,expDate,strike,type,quantity,date,price'];
  longs.forEach(o => hdrU.push(['buy_open', o.id, o.Symbol, o.expDate, o.strike, o.type, o.qty, o.date, o.price].join(',')));
  shorts.forEach(o => hdrU.push(['sell_open', o.id, o.Symbol, o.expDate, o.strike, o.type, o.qty, o.date, o.price].join(',')));
  fs.writeFileSync('unmatched_options.csv', hdrU.join('\n'));
  console.log('Wrote unmatched_options.csv');
}

runMatch();
