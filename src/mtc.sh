#!/bin/bash
export PYSPARK_PYTHON="/usr/local/python3.7.4/bin/python3"
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
parentDirForScriptSelf=$(cd "$(dirname "$0")"; cd ".."; pwd)

cd ${baseDirForScriptSelf}
rm ./libs.zip
zip -r ./libs.zip ../lib/

/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit  \
    --driver-memory 3g \
    --executor-memory 15g \
    --executor-cores 3 \
    --queue "root.users.fin.fin" \
    --py-files ${baseDirForScriptSelf}/libs.zip \
    --conf spark.yarn.executor.memoryOverhead=6g \
    --conf spark.driver.memoryOverhead=6g \
    --conf spark.sql.autoBroadcastJionThreshold=500485760 \
    --conf spark.network.timeout=800000 \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.rpc.message.maxSize=500 \
    --conf spark.rpc.askTimeout=600 \
    --conf spark.executor.heartbeatInterval=60000 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=100 \
    --conf spark.dynamicAllocation.executorIdleTimeout=100s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=2s \
    --conf spark.default.parallelism=400 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  ${baseDirForScriptSelf}/data_extractor.py $@

