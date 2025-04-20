const sqlite3 = require('sqlite3').verbose();

// Map instrument types to destination tables
const TABLE_MAP = {
    'Equity Option': 'tbl_EquityOptions',
    'Equity': 'tbl_Equities',
    'Future Option': 'tbl_FuturesOptions',
    'Future': 'tbl_Futures',
};

// Columns as in the destination tables (underscore style)
const DEST_COLUMNS = [
    'Date',
    'Type',
    'SubType',
    'Action',
    'Symbol',
    'Instrument_Type',
    'Description',
    'Value',
    'Quantity',
    'Average_Price',
    'Commissions',
    'Fees',
    'Multiplier',
    'Root_Symbol',
    'Underlying_Symbol',
    'Expiration_Date',
    'Strike_Price',
    'Call_or_Put',
    'Order_Number',
    'Total',
    'Currency',
    'hash_id'
];

// Mapping from rawdata columns to destination columns
const RAW_TO_DEST = {
    'Date': 'Date',
    'Type': 'Type',
    'Sub_Type': 'SubType',
    'Action': 'Action',
    'Symbol': 'Symbol',
    'Instrument_Type': 'Instrument_Type',
    'Description': 'Description',
    'Value': 'Value',
    'Quantity': 'Quantity',
    'Average_Price': 'Average_Price',
    'Commissions': 'Commissions',
    'Fees': 'Fees',
    'Multiplier': 'Multiplier',
    'Root_Symbol': 'Root_Symbol',
    'Underlying_Symbol': 'Underlying_Symbol',
    'Expiration_Date': 'Expiration_Date',
    'Strike_Price': 'Strike_Price',
    'Call_or_Put': 'Call_or_Put',
    'Order_Number': 'Order_Number',
    'Total': 'Total',
    'Currency': 'Currency',
    'hash_id': 'hash_id'
};

const db = new sqlite3.Database('tastytrades.db', (err) => {
    if (err) {
        console.error('Error opening database:', err);
        process.exit(1);
    }
    console.log('Connected to SQLite database : ' + db.filename);
    copyRows();
});

const dbexecute = async (db, sql, params = []) => {
    if (params && params.length > 0) {
      return new Promise((resolve, reject) => {
        db.run(sql, params, (err) => {
          if (err) reject(err);
          resolve();
        });
      });
    }
    return new Promise((resolve, reject) => {
      db.exec(sql, (err) => {
        if (err) reject(err);
        resolve();
      });
    });
  };
  
const fetchAll = async (db, sql, params) => {
    return new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => {
        if (err) reject(err);
        resolve(rows);
      });
    });
  };
  
const fetchFirst = async (db, sql, params) => {
    return new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => {
        if (err) reject(err);
        resolve(row);
      });
    });
  };

function copyRows() {
    db.serialize(async () => {
        try {
            await dbexecute(db, 'BEGIN TRANSACTION');
            let totalCopied = 0;
            for (const [instrType, destTable] of Object.entries(TABLE_MAP)) {
                // Build the SELECT part by mapping destination columns to raw data columns
                const selectCols = DEST_COLUMNS.map(destCol => {
                    // Find the raw column that maps to this destCol
                    const rawCol = Object.keys(RAW_TO_DEST).find(key => RAW_TO_DEST[key] === destCol);
                    // If not found, use NULL as fallback
                    return rawCol ? `"${rawCol}"` : 'NULL';
                });
                const sql = `INSERT INTO ${destTable} (${DEST_COLUMNS.join(', ')})\n` +
                    `SELECT ${selectCols.join(', ')} FROM tbl_rawdata WHERE "Instrument_Type" = ?\n` +
                    `AND hash_id NOT IN (SELECT hash_id FROM ${destTable})`;
                // console.log(`Executing SQL: ${sql}`);
                try {
                    // Optionally, count inserted rows first.
                    let info = await fetchFirst(db, `SELECT count(*) as cnt FROM tbl_rawdata WHERE "Instrument_Type" = "${instrType}"\n` +
                    `AND hash_id NOT IN (SELECT hash_id FROM ${destTable})`);
                    totalCopied += info.cnt;
                    // Now execute the INSERT.
                    await dbexecute(db, sql, [instrType]);
                    console.log(`Copied ${info.cnt} rows to ${destTable}.`);
                } catch (err) {
                    console.error(`Error inserting into ${destTable} in database ${db.filename}:`, err);
                }
            }
            await dbexecute(db, 'COMMIT');
            console.log(`Copy completed ${totalCopied} rows.`);
        } catch (err) {
            console.error('Error during copyRows transaction in database ' + db.filename + ':', err);
            try { await dbexecute(db, 'ROLLBACK'); } catch {}
        }
        db.close();
    });
}