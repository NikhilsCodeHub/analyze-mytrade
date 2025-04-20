const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');

// Open database
const db = new sqlite3.Database('./tastytrades.db', err => {
    if (err) throw err;
    console.log('Connected to DB');
    runMatch();
});

/**
 * Action helpers
 */
function isBuy(trade) {
    return trade.Action.toLowerCase().includes('buy');
}

function isSell(trade) {
    return trade.Action.toLowerCase().includes('sell');
}

function runMatch() {
    // Fetch all futures trades ordered by date, include symbol and expiration
    db.all(`SELECT rowid AS id, Date, Symbol, Expiration_Date AS expDate, Action, Quantity AS qty, Average_Price AS price, round((Commissions + Fees)/Quantity,2) AS costPerUnit, round(Total/Quantity,2) AS totalPerUnit, hash_id FROM tbl_Futures ORDER BY Date ASC`, (err, rows) => {
        if (err) throw err;
        // per-symbol queues for unmatched opens
        const openLongsMap = {};
        const openShortsMap = {};
        const matches = [];
        for (const trade of rows) {
            let qty = trade.qty;
            const key = `${trade.Symbol}|${trade.expDate}`;
            if (isBuy(trade)) {
                if (!openShortsMap[key]) openShortsMap[key] = [];
                const queue = openShortsMap[key];
                // close prior shorts for this symbol
                while (qty > 0 && queue.length > 0) {
                    const open = queue[0];
                    const matchQty = Math.min(open.qty, qty);
                    matches.push({symbol: trade.Symbol, expDate: trade.expDate, openId: open.id, closeId: trade.id, quantity: matchQty, openDate: open.date, closeDate: trade.Date, openCost: open.costPerUnit, closeCost: trade.costPerUnit, closeTotal: trade.totalPerUnit, price: trade.price, openHashId: open.hash_id, closeHashId: trade.hash_id});
                    open.qty -= matchQty;
                    qty -= matchQty;
                    if (open.qty === 0) queue.shift();
                }
                // remaining opens new long
                if (qty > 0) {
                    if (!openLongsMap[key]) openLongsMap[key] = [];
                    openLongsMap[key].push({id: trade.id, qty, date: trade.Date, costPerUnit: trade.costPerUnit, price: trade.price, totalPerUnit: trade.totalPerUnit, Symbol: trade.Symbol, expDate: trade.expDate, hash_id: trade.hash_id});
                }
            } else if (isSell(trade)) {
                if (!openLongsMap[key]) openLongsMap[key] = [];
                const queue = openLongsMap[key];
                // close prior longs for this symbol
                while (qty > 0 && queue.length > 0) {
                    const open = queue[0];
                    const matchQty = Math.min(open.qty, qty);
                    matches.push({symbol: trade.Symbol, expDate: trade.expDate, openId: open.id, closeId: trade.id, quantity: matchQty, openDate: open.date, closeDate: trade.Date, openCost: open.costPerUnit, closeCost: trade.costPerUnit, closeTotal: trade.totalPerUnit, price: trade.price, openHashId: open.hash_id, closeHashId: trade.hash_id});
                    open.qty -= matchQty;
                    qty -= matchQty;
                    if (open.qty === 0) queue.shift();
                }
                // remaining opens new short
                if (qty > 0) {
                    if (!openShortsMap[key]) openShortsMap[key] = [];
                    openShortsMap[key].push({id: trade.id, qty, date: trade.Date, costPerUnit: trade.costPerUnit, price: trade.price, totalPerUnit: trade.totalPerUnit, Symbol: trade.Symbol, expDate: trade.expDate, hash_id: trade.hash_id});
                }
            }
        }
        console.log(`Matched pairs: ${matches.length}`);
        //console.table(matches);
        // flatten unmatched opens
        const unmatchedLongs = Object.values(openLongsMap).flat();
        const unmatchedShorts = Object.values(openShortsMap).flat();
        if (unmatchedLongs.length) {
            console.log(`Unmatched long opens: ${unmatchedLongs.length}`);
            console.table(unmatchedLongs);
        }
        if (unmatchedShorts.length) {
            console.log(`Unmatched short opens: ${unmatchedShorts.length}`);
            console.table(unmatchedShorts);
        }
        // export CSVs
        exportCsv(matches, unmatchedLongs, unmatchedShorts);
        db.close();
    });
}

/**
 * Export matches and unmatched opens to CSV
 */
function exportCsv(matches, openLongs, openShorts) {
    // matched
    const matchCsv = ['symbol,expDate,openId,closeId,quantity,openDate,closeDate,openCost,closeCost,openTotal,closeTotal,price,openHashId,closeHashId'];
    matches.forEach(m => matchCsv.push([m.symbol, m.expDate, m.openId, m.closeId, m.quantity, m.openDate, m.closeDate, m.openCost, m.closeCost, m.closeTotal, m.price, m.openHashId, m.closeHashId].join(',')));
    fs.writeFileSync('matched_futures_trades.csv', matchCsv.join('\n'));
    console.log('Wrote matched_futures_trades.csv');
    // unmatched opens
    const unmatchedCsv = ['side,id,symbol,expDate,quantity,date,price,hashId'];
    openLongs.forEach(o => unmatchedCsv.push(['buy_open', o.id, o.Symbol, o.expDate, o.qty, o.date, o.price, o.hash_id].join(',')));
    openShorts.forEach(o => unmatchedCsv.push(['sell_open', o.id, o.Symbol, o.expDate, o.qty, o.date, o.price, o.hash_id].join(',')));
    fs.writeFileSync('unmatched_futures_trades.csv', unmatchedCsv.join('\n'));
    console.log('Wrote unmatched_futures_trades.csv');
}
