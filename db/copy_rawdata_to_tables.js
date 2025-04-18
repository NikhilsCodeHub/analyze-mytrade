const sqlite3 = require('sqlite3').verbose();

// Map instrument types to destination tables
const TABLE_MAP = {
    'Equity Option': 'tbl_EquityOptions',
    'Equity': 'tbl_Equities',
    'Futures Option': 'tbl_FuturesOptions',
    'Futures': 'tbl_Futures',
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
];

// Mapping from rawdata columns to destination columns
const RAW_TO_DEST = {
    'Date': 'Date',
    'Type': 'Type',
    'Sub Type': 'SubType',
    'Action': 'Action',
    'Symbol': 'Symbol',
    'Instrument Type': 'Instrument_Type',
    'Description': 'Description',
    'Value': 'Value',
    'Quantity': 'Quantity',
    'Average Price': 'Average_Price',
    'Commissions': 'Commissions',
    'Fees': 'Fees',
    'Multiplier': 'Multiplier',
    'Root Symbol': 'Root_Symbol',
    'Underlying Symbol': 'Underlying_Symbol',
    'Expiration Date': 'Expiration_Date',
    'Strike Price': 'Strike_Price',
    'Call or Put': 'Call_or_Put',
    'Order #': 'Order_Number',
    'Total': 'Total',
    'Currency': 'Currency',
};

const db = new sqlite3.Database('tastytrades.db', (err) => {
    if (err) {
        console.error('Error opening database:', err);
        process.exit(1);
    }
    console.log('Connected to SQLite database');
    copyRows();
});

function copyRows() {
    db.all('SELECT * FROM tbl_rawdata WHERE "Instrument Type" IS NOT NULL', (err, rows) => {
        if (err) {
            console.error('Error querying tbl_rawdata:', err);
            process.exit(1);
        }
        let copied = 0, skipped = 0;
        db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            rows.forEach(row => {
                const instrType = row['Instrument Type'];
                const destTable = TABLE_MAP[instrType];
                if (!destTable) {
                    skipped++;
                    return;
                }
                // Prepare values in destination column order
                const values = DEST_COLUMNS.map(destCol => {
                    // Find the raw column that maps to this destCol
                    const rawCol = Object.keys(RAW_TO_DEST).find(key => RAW_TO_DEST[key] === destCol);
                    return row[rawCol] !== undefined ? row[rawCol] : null;
                });
                const placeholders = DEST_COLUMNS.map(() => '?').join(', ');
                const sql = `INSERT INTO ${destTable} (${DEST_COLUMNS.join(', ')}) VALUES (${placeholders})`;
                db.run(sql, values, (err) => {
                    if (err) {
                        console.error(`Error inserting into ${destTable}:`, err);
                    } else {
                        copied++;
                    }
                });
            });
            db.run('COMMIT', (err) => {
                if (err) {
                    console.error('Error committing transaction:', err);
                } else {
                    console.log(`Copied ${copied} rows. Skipped ${skipped} rows.`);
                }
                db.close();
            });
        });
    });
}
