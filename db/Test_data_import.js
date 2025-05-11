'use strict';
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const sqlite3 = require('sqlite3').verbose();
const { promisify } = require('util');

(async () => {
  try {
    const dbFilePath = path.join(__dirname, 'trades.db');
    const csvDir = path.join(__dirname, '.');

    // gather CSVs
/*     const csvFiles = fs.readdirSync(csvDir).filter(f => f.endsWith('.csv'));
    if (!csvFiles.length) {
      console.log('No CSV files found in', csvDir);
      return;
    } */


      const arg = process.argv[2];
      if (!arg) {
          console.error('Usage: node import_trades.js <file1.csv,file2.csv,...>');
          process.exit(1);
      }
      const csvFiles = arg.split(',').map(f => f.trim()).filter(f => f.length > 0);
      if (csvFiles.length === 0) {
          console.error('No files specified.');
          process.exit(1);
      }



    // parse header from first CSV
    const firstHeaderLine = fs.readFileSync(path.join(csvDir, csvFiles[0]), 'utf-8').split(/\r?\n/)[0];
    const originalHeaders = firstHeaderLine.split(',').map(h => h.trim());

    // sanitize headers
    const sanitizeHeader = header => {
      let h = header.replace(/#/g, 'Number');
      h = h.trim().replace(/\s+/g, '');
      h = h.replace(/[^\w-]/g, '');
      return h.replace(/-+/g, '').replace(/^-+|-+$/g, '');
    };
    const sanitizedHeaders = originalHeaders.map(sanitizeHeader);

    // define column types
    const numericCols = new Set(['Value','Quantity','Average-Price','Commissions','Fees','Multiplier','Strike-Price','Total']);
    const columnDefs = sanitizedHeaders.map(col => {
      if (col === 'Quantity') return `${col} INTEGER`;
      if (numericCols.has(col)) return `${col} REAL`;
      return `${col} TEXT`;
    });

    // open DB and promisify
    const db = new sqlite3.Database(dbFilePath);
    const runAsync = promisify(db.run.bind(db));
    const getAsync = promisify(db.get.bind(db));

    // create final table
    console.log('Creating final table...');
    //console.log(`CREATE TABLE IF NOT EXISTS tbl_finaltrades (${columnDefs.join(', ')});`);

    await runAsync(`CREATE TABLE IF NOT EXISTS tbl_finaltrades (${columnDefs.join(', ')});`);
    
    // process each file
    for (const file of csvFiles) {
      const filePath = path.join(csvDir, file);

      // drop/create temp table
      await runAsync('DROP TABLE IF EXISTS temp_import;');
      await runAsync(`CREATE TABLE temp_import (${columnDefs.join(', ')});`);

      // stream parse and insert
      const parser = fs.createReadStream(filePath).pipe(parse({ columns: true, trim: true }));
      const stmt = db.prepare(`INSERT INTO temp_import (${sanitizedHeaders.join(',')}) VALUES (${sanitizedHeaders.map(()=>'?').join(',')});`);
     
 
      const stmtRun = promisify(stmt.run.bind(stmt));

      for await (const record of parser) {
        const vals = originalHeaders.map((orig,i) => {
          const key = sanitizedHeaders[i];
          let v = record[orig];
          if (numericCols.has(key)) v = v ? Number(v.replace(/,/g, '')) : null;
          return v;
        });
        await stmtRun(vals);
      }
      stmt.finalize();

      // counts and import
      const { count: tempCount } = await getAsync('SELECT COUNT(*) as count FROM temp_import;');
      const { count: newCount } = await getAsync(
        `SELECT COUNT(*) as count FROM (SELECT ${sanitizedHeaders.join(',')} FROM temp_import EXCEPT SELECT ${sanitizedHeaders.join(',')} FROM tbl_finaltrades);`
      );
      await runAsync(
        `INSERT INTO tbl_finaltrades (${sanitizedHeaders.join(',')}) SELECT ${sanitizedHeaders.join(',')} FROM temp_import EXCEPT SELECT ${sanitizedHeaders.join(',')} FROM tbl_finaltrades;`
      );
    //  console.log(`INSERT INTO tbl_finaltrades (${sanitizedHeaders.join(',')}) SELECT ${sanitizedHeaders.join(',')} FROM temp_import EXCEPT SELECT ${sanitizedHeaders.join(',')} FROM tbl_finaltrades;`);

      console.log(`[${new Date().toISOString()}] ${file}: temp rows ${tempCount}, imported ${newCount}`);
    }
    db.close();
  } catch (err) {
    console.error('Error:', err);
  }
})();
