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
    db.run("Delete from tbl_OpenPositions;");
    // Fetch all futures trades ordered by date, include symbol and expiration
    db.all(`SELECT rowid AS id, Date, Description, Symbol, Expiration_Date AS expDate, Action, Quantity AS qty, Average_Price AS price, round((Commissions + Fees)/Quantity,2) AS costPerUnit, round(Total/Quantity,2) AS totalPerUnit, hash_id FROM tbl_Futures WHERE isMatched != 1 ORDER BY Date ASC`, (err, rows) => {
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
                    matches.push({symbol: trade.Symbol, expDate: trade.expDate, Description: open.Description + " | " + trade.Action + " " + trade.Description.substring(trade.Description.indexOf('@')), openId: open.id, closeId: trade.id, quantity: matchQty, openDate: open.date, closeDate: trade.Date, openCost: open.costPerUnit, closeCost: trade.costPerUnit, closeTotal: trade.totalPerUnit, price: trade.price, openHashId: open.hash_id, closeHashId: trade.hash_id});
                    open.qty -= matchQty;
                    qty -= matchQty;
                    if (open.qty === 0) queue.shift();
                }
                // remaining opens new long
                if (qty > 0) {
                    if (!openLongsMap[key]) openLongsMap[key] = [];
                    openLongsMap[key].push({id: trade.id, qty, date: trade.Date, Description: trade.Action + " " + trade.Description.substring(trade.Description.indexOf('@')), costPerUnit: trade.costPerUnit, price: trade.price, totalPerUnit: trade.totalPerUnit, Symbol: trade.Symbol, expDate: trade.expDate, hash_id: trade.hash_id});
                }
            } else if (isSell(trade)) {
                if (!openLongsMap[key]) openLongsMap[key] = [];
                const queue = openLongsMap[key];
                // close prior longs for this symbol
                while (qty > 0 && queue.length > 0) {
                    const open = queue[0];
                    const matchQty = Math.min(open.qty, qty);
                    matches.push({symbol: trade.Symbol, expDate: trade.expDate, openId: open.id, closeId: trade.id, openDate: open.date, closeDate: trade.Date, quantity: matchQty, openCost: open.costPerUnit, closeCost: trade.costPerUnit, closeTotal: trade.totalPerUnit, price: trade.price, Description: open.Description + " | " + trade.Action + " " + trade.Description.substring(trade.Description.indexOf('@')),openHashId: open.hash_id, closeHashId: trade.hash_id});
                    open.qty -= matchQty;
                    qty -= matchQty;
                    if (open.qty === 0) queue.shift();
                }
                // remaining opens new short
                if (qty > 0) {
                    if (!openShortsMap[key]) openShortsMap[key] = [];
                    openShortsMap[key].push({id: trade.id, qty, date: trade.Date, Description: trade.Action + " " + trade.Description.substring(trade.Description.indexOf('@')), costPerUnit: trade.costPerUnit, price: trade.price, totalPerUnit: trade.totalPerUnit, Symbol: trade.Symbol, expDate: trade.expDate, hash_id: trade.hash_id});
                }
            } else if (trade.Description && trade.Description.toLowerCase().includes('mark to market')) {
                // mark-to-market adjustment entry
                matches.push({
                    symbol: trade.Symbol,
                    Description: trade.Description,
                    openId: trade.id,
                    closeId: trade.id,
                    openDate: trade.Date,
                    closeDate: trade.Date,
                    quantity: trade.qty,
                    openCost: 0,
                    closeCost: 0,
                    price: trade.price,
                    //gainPerUnit: trade.price * trade.qty,
                    closeTotal: trade.totalPerUnit * trade.qty,
                    openHashId: trade.hash_id,
                    closeHashId: trade.hash_id
                });
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
        // export CSVs -- Used for Testing.
        //exportCsv(matches, unmatchedLongs, unmatchedShorts);
        writeToDb(matches, unmatchedLongs, unmatchedShorts);
        db.close();
    });
}

/**
 * Export matches and unmatched opens to CSV
 */
function exportCsv(matches, openLongs, openShorts) {
    // matched
    const matchCsv = ['symbol,expDate,openId,closeId,quantity,openDate,closeDate,openCost,closeCost,gainPerUnit,netProceeds,openHashId,closeHashId'];
    matches.forEach(m => matchCsv.push([m.symbol, m.expDate, m.openId, m.closeId, m.quantity, m.openDate, m.closeDate, m.openCost, m.closeCost, m.price,((m.price + m.openCost + m.closeCost) * m.quantity).toFixed(2), m.openHashId, m.closeHashId].join(',')));
    fs.writeFileSync('matched_futures_trades.csv', matchCsv.join('\n'));
    console.log('Wrote matched_futures_trades.csv');
    // unmatched opens
    const unmatchedCsv = ['side,id,symbol,expDate,quantity,date,price,hashId'];
    openLongs.forEach(o => unmatchedCsv.push(['buy_open', o.id, o.Symbol, o.expDate, o.qty, o.date, o.price, o.hash_id].join(',')));
    openShorts.forEach(o => unmatchedCsv.push(['sell_open', o.id, o.Symbol, o.expDate, o.qty, o.date, o.price, o.hash_id].join(',')));
    fs.writeFileSync('unmatched_futures_trades.csv', unmatchedCsv.join('\n'));
    console.log('Wrote unmatched_futures_trades.csv');
}

/**
 * Persist matched and unmatched trades to DB tables and update isMatched
 */
function writeToDb(matches, openLongs, openShorts) {
    db.serialize(() => {
        /* Only needed first time when DB is created.
        //-------------------------------------------
        db.run("CREATE TABLE IF NOT EXISTS tbl_MatchedTrades (symbol TEXT, Description TEXT, openId INTEGER, closeId INTEGER, openDate TEXT, closeDate TEXT, quantity INTEGER, openCost REAL, closeCost REAL, gainPerUnit REAL, netProceeds REAL, openHashId TEXT, closeHashId TEXT)");

        db.run("CREATE TABLE IF NOT EXISTS tbl_OpenPositions (side TEXT, id INTEGER, symbol TEXT, openDate TEXT, quantity INTEGER, price REAL, hashId TEXT)");
        */

        // ensure no duplicate matched entries
        db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_matched_open_close ON tbl_MatchedTrades(openId, closeId)");
        // ensure no duplicate open positions
        db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_open_positions ON tbl_OpenPositions(side, id)");
        
        const mt = db.prepare("INSERT OR IGNORE INTO tbl_MatchedTrades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
        matches.forEach(m => {
            const gainPerUnit = m.price;
            const netProceeds = ((m.price + m.openCost + m.closeCost) * m.quantity).toFixed(2);
            mt.run(
                m.symbol, m.Description, m.openId, m.closeId, m.openDate, m.closeDate, m.quantity, m.openCost, m.closeCost, gainPerUnit, netProceeds, m.openHashId, m.closeHashId,
                (err) => {
                    // ignore unique constraint failures
                    if (err && err.code !== 'SQLITE_CONSTRAINT') console.error(err);
                }
            );
        });
        mt.finalize();
        const op = db.prepare("INSERT INTO tbl_OpenPositions VALUES (?,?,?,?,?,?,?,?)");
        openLongs.forEach(o => op.run('buy_open', o.id, o.Symbol, o.openDate, o.qty, o.price, o.hash_id));
        openShorts.forEach(o => op.run('sell_open', o.id, o.Symbol, o.openDate, o.qty, o.price, o.hash_id));
        op.finalize();
        // ensure isMatched column exists (ignore if exists)
        // db.run("ALTER TABLE tbl_Futures ADD COLUMN isMatched INTEGER DEFAULT 0", () => {});
        // mark matched rows
        matches.forEach(m => {
            db.run("UPDATE tbl_Futures SET isMatched = 1 WHERE rowid IN (?,?)", m.openId, m.closeId);
        });
    });
}
