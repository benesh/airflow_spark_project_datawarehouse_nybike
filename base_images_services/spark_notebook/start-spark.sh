#!/bin/bash

source ~/.profile

# Start Spark master
echo "Starting Spark Master..."
/opt/spark/sbin/start-master.sh

# Start Spark worker
echo "Starting Spark Worker..."
/opt/spark/sbin/start-worker.sh spark://localhost:7077

# Verify Spark processes
echo "Verifying Spark processes..."
ps aux | grep spark

# echo 'Hello World'; 
# sleep infinity

# # Start Jupyter Notebook
echo "Starting Jupyter Notebook..."
cd /home/jovyan/work
jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser