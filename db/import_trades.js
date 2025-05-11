const fs = require('fs');
const { parse } = require('csv-parse');
const db = require('./connection');
const crypto = require('crypto');

// Create table if it doesn't exist
const createTable = `
CREATE TABLE IF NOT EXISTS tbl_rawdata (
    Date TEXT,
    Type TEXT,
    Sub_Type TEXT,
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
    hash_id TEXT
)`;

const createTempTable = `
CREATE TABLE IF NOT EXISTS tbl_tempData (
    Date TEXT,
    Type TEXT,
    Sub_Type TEXT,
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
    hash_id TEXT
)`;

db.serialize(() => {
    db.run(createTable, (err) => {
        if (err) {
            console.error('Error creating table:', err);
            process.exit(1);
        }
        console.log('Table created or already exists');
    });

    db.run(createTempTable, (err) => {
        if (err) {
            console.error('Error creating temp table:', err);
            process.exit(1);
        }
        console.log('Temp table created or already exists');
        processFiles();
    });
});

function processFiles() {
    const files = parseArguments();
    processAllFiles(files);
}

function parseArguments() {
    // parse command line arguments. Accepting upto 12 files at a time.
    const arg = process.argv.slice(2, 12).join(',');
    if (!arg) {
        console.error('Usage: node import_trades.js <file1.csv,file2.csv,...>');
        process.exit(1);
    }
    const files = arg.split(',').map(f => f.trim()).filter(f => f.length > 0);
    if (files.length === 0) {
        console.error('No files specified.');
        process.exit(1);
    }
    console.log(`Received ${files.length} files: ${files.join(', ')}`);
    return files;
}

function processAllFiles(files) {
    let idx = 0;
    const total = files.length;
    function processNext() {
        if (idx >= total) {
            // All files done: clear temp table
            db.run('DELETE FROM tbl_tempData', (err) => {
                if (err) console.error('Error truncating temp table:', err);
                else console.log('Temp table cleared');
            });
            console.log('All files processed');
            return;
        }
        const filename = files[idx];
        console.log(`Starting processing ${idx + 1} of ${total}: ${filename}`);
        parseCsvAndNormalize(
            filename,
            (records) => {
                importRecordsToTempTable(filename, records, () => {
                    idx++;
                    processNext();
                });
            },
            (err) => {
                console.error(`Error parsing CSV for ${filename}:`, err);
                idx++;
                processNext();
            }
        );
    }
    processNext();
}

function parseCsvAndNormalize(filename, onSuccess, onError) {
    const { parse } = require('csv-parse');
    const fs = require('fs');
    const parser = parse({ columns: true, skip_empty_lines: true, trim: true });
    const records = [];
    parser.on('readable', () => {
        let record;
        while ((record = parser.read()) !== null) {
            // Clean up numeric values
            record.Value = parseFloat(record.Value?.replace(/[^-0-9.]/g, '') || 0);
            record.Quantity = parseFloat(record.Quantity);
            record['Average Price'] = parseFloat(record['Average Price']?.replace(/[^-0-9.]/g, '') || 0);
            record.Commissions = parseFloat(record.Commissions);
            record.Fees = parseFloat(record.Fees);
            record.Multiplier = parseInt(record.Multiplier);
            record['Strike Price'] = record['Strike Price'] ? parseFloat(record['Strike Price']) : null;
            record.Total = parseFloat(record.Total?.replace(/[^-0-9.]/g, '') || 0);
            const hashId = crypto.createHash('md5').update(JSON.stringify(record)).digest('hex');
            records.push([
                record.Date,
                record.Type,
                record['Sub Type'],
                record.Action,
                record.Symbol,
                record["Instrument Type"],
                record.Description,
                record.Value,
                record.Quantity,
                record['Average Price'],
                record.Commissions,
                record.Fees,
                record.Multiplier,
                record['Root Symbol'],
                record['Underlying Symbol'],
                record['Expiration Date'],
                record['Strike Price'],
                record['Call or Put'],
                record['Order #'],
                record.Total,
                record.Currency,
                hashId
            ]);
        }
    });
    parser.on('error', (err) => {
        onError(err);
    });
    parser.on('end', () => {
        onSuccess(records);
        console.log(`Successfully parsed ${records.length} records from ${filename}`);
    });
    fs.createReadStream(filename).pipe(parser);
}

function importRecordsToTempTable(filename, records, onComplete){
    db.serialize(() => {
        db.run('BEGIN TRANSACTION');
        const stmt = db.prepare(`
            INSERT INTO tbl_tempData (
                Date, Type, "Sub_Type", Action, Symbol, "Instrument_Type",
                Description, Value, Quantity, "Average_Price", Commissions,
                Fees, Multiplier, "Root_Symbol", "Underlying_Symbol",
                "Expiration_Date", "Strike_Price", "Call_or_Put", "Order_Number",
                Total, Currency, hash_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        let processed = 0;
        for (const record of records) {
            stmt.run(record, (err) => {
                if (err) {
                    console.error('Error inserting record:', err);
                } else {
                    processed++;
                }
            });
        }
        stmt.finalize();
        db.run('COMMIT', (err) => {
            if (err) {
                console.error('Error committing transaction:', err);
            } else {
                console.log(`Completed staging ${processed} records from ${filename}`);
                // Count the new records to be inserted.
                const countSQL = `SELECT COUNT(*) AS newCount FROM tbl_tempData WHERE hash_id NOT IN (SELECT hash_id FROM tbl_rawdata);`;
                db.get(countSQL, (err, row) => {
                    if (err) {
                        console.error('Error getting new row count from temp table for file : ', filename, err);
                    } else {
                        console.log(`${row.newCount} Rows : will be inserted into rawdata from file ${filename}.`);
                    }
                });
                // Insert new records from temp table into rawdata. Remove duplicates.
                const insertSQL = `
                    INSERT INTO tbl_rawdata
                    (Date, Type, Sub_Type, Action, Symbol, Instrument_Type, Description, Value, Quantity, Average_Price, Commissions, Fees, Multiplier, Root_Symbol, Underlying_Symbol, Expiration_Date, Strike_Price, Call_or_Put, Order_Number, Total, Currency, hash_id)
                    SELECT Date, Type, Sub_Type, Action, Symbol, Instrument_Type, Description, Value, Quantity, Average_Price, Commissions, Fees, Multiplier, Root_Symbol, Underlying_Symbol, Expiration_Date, Strike_Price, Call_or_Put, Order_Number, Total, Currency, hash_id
                    FROM tbl_tempData
                    WHERE hash_id NOT IN (SELECT hash_id FROM tbl_rawdata);
                `;
                db.run(insertSQL, (err) => {
                    if (err) {
                        console.error('Error inserting into rawdata from file : ', filename, err);
                    } else {
                        console.log('Inserted new records into rawdata from file : ', filename);
                        if (onComplete) onComplete();
                    }
                });
            }
        });
    });
}
