#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"
cd /opt/notebooks
if [ "$SPARK_WORKLOAD" == "master" ];
then
  echo "part of spark master"
  start-master.sh \
    --port $SPARK_MASTER_PORT 

  cd /opt/notebooks
  $PYSPARK_DRIVER_PYTHON_OPTS

elif [[ $SPARK_WORKLOAD == "worker" ]];
# if $SPARK_WORKLOAD contains substring "worker". try 
# try "worker-1", "worker-2" etc.
then

  # start-worker.sh spark://spark-master:7077
  start-worker.sh $SPARK_MASTER

elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi
