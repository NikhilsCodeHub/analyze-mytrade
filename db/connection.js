const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('db/tastytrades.db', (err) => {
  if (err) {
    console.error('Failed to connect to DB:', err);
    throw err;
  }
  console.log('Connected to DB ' + db.filename);
});

module.exports = db;
