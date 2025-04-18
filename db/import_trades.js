const fs = require('fs');
const { parse } = require('csv-parse');
const sqlite3 = require('sqlite3').verbose();

// Create or open SQLite database
const db = new sqlite3.Database('./tastytrades.db', (err) => {
    if (err) {
        console.error('Error opening database:', err);
        process.exit(1);
    }
    console.log('Connected to SQLite database');
});

// Create table if it doesn't exist
const createTable = `
CREATE TABLE IF NOT EXISTS tbl_rawdata (
    Date TEXT,
    Type TEXT,
    "Sub Type" TEXT,
    Action TEXT,
    Symbol TEXT,
    "Instrument Type" TEXT,
    Description TEXT,
    Value REAL,
    Quantity REAL,
    "Average Price" REAL,
    Commissions REAL,
    Fees REAL,
    Multiplier INTEGER,
    "Root Symbol" TEXT,
    "Underlying Symbol" TEXT,
    "Expiration Date" TEXT,
    "Strike Price" REAL,
    "Call or Put" TEXT,
    "Order #" TEXT,
    Total REAL,
    Currency TEXT
)`;

db.run(createTable, (err) => {
    if (err) {
        console.error('Error creating table:', err);
        process.exit(1);
    }
    console.log('Table created or already exists');
    processFiles();
});

// Modularized import_trades.js

function parseArguments() {
    const arg = process.argv[2];
    if (!arg) {
        console.error('Usage: node import_trades.js <file1.csv,file2.csv,...>');
        process.exit(1);
    }
    const files = arg.split(',').map(f => f.trim()).filter(f => f.length > 0);
    if (files.length === 0) {
        console.error('No files specified.');
        process.exit(1);
    }
    return files;
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
            record.AveragePrice = parseFloat(record['Average Price']?.replace(/[^-0-9.]/g, '') || 0);
            record.Commissions = parseFloat(record.Commissions);
            record.Fees = parseFloat(record.Fees);
            record.Multiplier = parseInt(record.Multiplier);
            record.StrikePrice = record['Strike Price'] ? parseFloat(record['Strike Price']) : null;
            record.Total = parseFloat(record.Total?.replace(/[^-0-9.]/g, '') || 0);
            records.push([
                record.Date,
                record.Type,
                record['Sub Type'],
                record.Action,
                record.Symbol,
                record['Instrument Type'],
                record.Description,
                record.Value,
                record.Quantity,
                record.AveragePrice,
                record.Commissions,
                record.Fees,
                record.Multiplier,
                record['Root Symbol'],
                record['Underlying Symbol'],
                record['Expiration Date'],
                record.StrikePrice,
                record['Call or Put'],
                record['Order #'],
                record.Total,
                record.Currency
            ]);
        }
    });
    parser.on('error', (err) => {
        onError(err);
    });
    parser.on('end', () => {
        onSuccess(records);
    });
    fs.createReadStream(filename).pipe(parser);
}

function importRecordsToDb(db, filename, records, onComplete) {
    db.serialize(() => {
        db.run('BEGIN TRANSACTION');
        const stmt = db.prepare(`
            INSERT INTO tbl_rawdata (
                Date, Type, "Sub Type", Action, Symbol, "Instrument Type",
                Description, Value, Quantity, "Average Price", Commissions,
                Fees, Multiplier, "Root Symbol", "Underlying Symbol",
                "Expiration Date", "Strike Price", "Call or Put", "Order #",
                Total, Currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        let processed = 0;
        function processRecords(idx) {
            if (idx >= records.length) {
                stmt.finalize();
                db.run('COMMIT', (err) => {
                    if (err) {
                        console.error('Error committing transaction:', err);
                    } else {
                        console.log(`Successfully imported ${processed} unique records from ${filename}`);
                    }
                    onComplete();
                });
                return;
            }
            const record = records[idx];
            db.get('SELECT 1 FROM tbl_rawdata WHERE Date = ? AND Description = ? AND Value = ? AND Total = ? LIMIT 1', [record[0], record[6], record[7], record[19]], (err, row) => {
                if (err) {
                    console.error('Error checking for duplicate:', err);
                    processRecords(idx + 1);
                } else if (row) {
                    processRecords(idx + 1);
                } else {
                    stmt.run(record, (err) => {
                        if (err) {
                            console.error('Error inserting record:', err);
                        } else {
                            processed++;
                        }
                        processRecords(idx + 1);
                    });
                }
            });
        }
        processRecords(0);
    });
}

function processAllFiles(db, files) {
    let fileIndex = 0;
    function nextFile() {
        if (fileIndex >= files.length) {
            console.log('All files processed');
            db.close();
            return;
        }
        const filename = files[fileIndex];
        console.log(`Processing ${filename}`);
        parseCsvAndNormalize(
            filename,
            (records) => {
                console.log(`Parsed ${records.length} records from ${filename}`);
                importRecordsToDb(db, filename, records, () => {
                    fileIndex++;
                    nextFile();
                });
            },
            (err) => {
                console.error(`Error parsing CSV for ${filename}:`, err);
                fileIndex++;
                nextFile();
            }
        );
    }
    nextFile();
}

function processFiles() {
    const files = parseArguments();
    processAllFiles(db, files);
}

