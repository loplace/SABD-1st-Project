#!/bin/bash
$SPARK_HOME/bin/spark-submit \
--class "queries.ThirdQuerySolver" \
--master "local" \
/sabd/jar/SABD-project1-1.0-SNAPSHOT.jar local parquet
