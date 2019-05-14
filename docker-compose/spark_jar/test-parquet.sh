#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "parser.WeatherRDDLoaderFromParquetFile" --master "local" /sabd/jar/SABD-project1-1.0-SNAPSHOT.jar local