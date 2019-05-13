#!/bin/bash
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$SPARK_HOME/sbin/start-all.sh

hdfs dfs -chmod -R 777 /