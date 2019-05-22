#!/bin/bash
$SPARK_HOME/bin/spark-submit \
--class "queries.FirstQuerySolver" \
--master yarn \
--deploy-mode cluster \
--driver-memory 2g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 4 \
/sabd/jar/SABD-project1-1.0-SNAPSHOT.jar yarn csv \
