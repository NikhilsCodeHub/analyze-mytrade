const express = require('express');
const router = express.Router();

// Mock data - in a real application, this would come from a database
const prescriptionData = {
    totalPrescriptions: "125,000",
    lastUpdated: '2025-03-25'
};

// GET endpoint for total prescriptions
router.get('/total', (req, res) => {
    res.json(prescriptionData);
});

module.exports = router;
