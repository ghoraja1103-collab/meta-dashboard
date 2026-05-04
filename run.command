#!/bin/bash

cd "/Users/rajatghosh/python/meta-dashboard"

source venv/bin/activate

# start streamlit in background
nohup "/Users/rajatghosh/python/meta-dashboard/venv/bin/python" -m streamlit run dashboard.py >/dev/null 2>&1 &

# WAIT until server is actually ready
echo "Waiting for server..."

for i in {1..10}
do
    sleep 1
    if lsof -i :8501 >/dev/null
    then
        echo "Server started"
        open http://localhost:8501
        exit 0
    fi
done

echo "Server failed to start"