#!/usr/bin/env bash

spark-submit --class qa.tools.monitor.App \
    --master yarn \
    --num-executors 1 \
    --deploy-mode client \
    --driver-memory 256m \
    --executor-memory 256m \
    --executor-cores 1 \
    --conf spark.driver.userClassPathFirst=true \
    --conf spark.executor.userClassPathFirst=true \
