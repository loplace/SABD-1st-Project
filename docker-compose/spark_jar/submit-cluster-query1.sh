#!/bin/bash
$SPARK_HOME/bin/spark-submit \
--class "queries.FirstQuerySolver" \
--master yarn \
--deploy-mode cluster \
--driver-memory 4g \
--executor-memory 2g \
--executor-cores 1 \
/sabd/jar/SABD-project1-1.0-SNAPSHOT.jar yarn csv \
