#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then

  echo "part of spark master"
  start-master.sh \
    --port $SPARK_MASTER_PORT 

elif [[ $SPARK_WORKLOAD == "worker" ]];

then
  # start-worker.sh spark://spark-master:7077
  start-worker.sh $SPARK_MASTER

elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi
