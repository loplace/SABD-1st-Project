#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "queries.FirstQuerySolver" --master "local" --jar /sabd/docker-compose-sabd.jar /sabd/SABD-project1-1.0-SNAPSHOT.jar
