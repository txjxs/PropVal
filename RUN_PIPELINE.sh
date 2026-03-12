#!/bin/bash
# Quick reference for running the ingestion pipeline

echo "PropVal Ingestion Pipeline Commands"
echo "===================================="
echo ""
echo "Fetch FOR_SALE listings:"
echo "  source venv/bin/activate && python src/ingest.py forSale"
echo ""
echo "Fetch SOLD listings:"
echo "  source venv/bin/activate && python src/ingest.py sold"
echo ""
echo "Default (uses forSale):"
echo "  source venv/bin/activate && python src/ingest.py"
echo ""
