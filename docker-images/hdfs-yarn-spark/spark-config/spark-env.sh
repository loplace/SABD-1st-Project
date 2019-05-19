#!/bin/bash
export SPARK_JAVA_OPTS=-Dspark.driver.port=53411
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_YARN_HOME/etc/hadoop
export SPARK_MASTER_IP=master

#export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=0.0.0.0:43211