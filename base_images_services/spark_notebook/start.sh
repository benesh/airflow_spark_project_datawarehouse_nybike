#!/bin/bash
set -e

# Start Spark master
$SPARK_HOME/sbin/start-master.sh --host $SPARK_MASTER_HOST

# Wait a bit for the master to start
sleep 3

# Start Spark worker (connects to the master)
$SPARK_HOME/sbin/start-worker.sh spark://$SPARK_MASTER_HOST:7077

# Start JupyterLab in the foreground (container will keep running as long as JupyterLab runs)
cd /workspace
exec jupyter lab --port-retries=0 --ip 0.0.0.0 --allow-root --config=/root/.jupyter/jupyter_notebook_config.py --debug
