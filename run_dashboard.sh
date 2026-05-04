#!/bin/bash

cd "$(dirname "$0")"

source venv/bin/activate

echo "🚀 Launching Meta Dashboard..."

streamlit run dashboard.py