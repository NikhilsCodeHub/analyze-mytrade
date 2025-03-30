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

function processFiles() {
    const files = [
        'tastytrade_transactions_history_230301_to_231231.csv',
        'tastytrade_transactions_history_250102_to_250329.csv'
    ];

    // Process each file sequentially
    let fileIndex = 0;
    processNextFile();

    function processNextFile() {
        if (fileIndex >= files.length) {
            console.log('All files processed');
            db.close();
            return;
        }

        const filename = files[fileIndex];
        console.log(`Processing ${filename}`);

        const parser = parse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        });

        const records = [];
        
        parser.on('readable', () => {
            let record;
            while ((record = parser.read()) !== null) {
                // Clean up numeric values
                record.Value = parseFloat(record.Value.replace(/[^-0-9.]/g, ''));
                record.Quantity = parseFloat(record.Quantity);
                record.AveragePrice = parseFloat(record['Average Price'].replace(/[^-0-9.]/g, ''));
                record.Commissions = parseFloat(record.Commissions);
                record.Fees = parseFloat(record.Fees);
                record.Multiplier = parseInt(record.Multiplier);
                record.StrikePrice = record['Strike Price'] ? parseFloat(record['Strike Price']) : null;
                record.Total = parseFloat(record.Total.replace(/[^-0-9.]/g, ''));
                
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
            console.error('Error parsing CSV:', err);
            process.exit(1);
        });

        parser.on('end', () => {
            console.log(`Parsed ${records.length} records from ${filename}`);
            
            // Begin transaction for better performance
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

                records.forEach(record => {
                    stmt.run(record, (err) => {
                        if (err) {
                            console.error('Error inserting record:', err);
                        }
                    });
                });

                stmt.finalize();
                
                db.run('COMMIT', (err) => {
                    if (err) {
                        console.error('Error committing transaction:', err);
                    } else {
                        console.log(`Successfully imported ${records.length} records from ${filename}`);
                        fileIndex++;
                        processNextFile();
                    }
                });
            });
        });

        fs.createReadStream(filename).pipe(parser);
    }
}
