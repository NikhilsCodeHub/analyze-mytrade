const express = require('express');
const path = require('path');

const app = express();
const port = 4000;

app.use(express.static(path.join(__dirname, 'build'))); // Serve static files from the 'build' folder

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});
