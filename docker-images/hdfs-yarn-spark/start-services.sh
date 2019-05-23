#!/bin/bash
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$SPARK_HOME/sbin/start-all.sh

sleep 30

hdfs dfs -chmod -R 777 /
hdfs dfs -mkdir /spark-events
hdfs dfs -mkdir /spark-logs
$SPARK_HOME/sbin/start-history-server.sh