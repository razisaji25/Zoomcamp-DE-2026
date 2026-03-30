#!/bin/bash

echo "=== RUN TRANSFORM ==="
python3 pipeline/transform.py

echo "=== LOAD TO DUCKDB ==="
python3 pipeline/load.py