const express = require('express');
const router = express.Router();

// Mock data - in a real application, this would come from a database
const memberData = {
    totalMembers: 850,
    lastUpdated: '2025-03-25'
};

// GET endpoint for total members
router.get('/total', (req, res) => {
    res.json(memberData);
});

module.exports = router;
