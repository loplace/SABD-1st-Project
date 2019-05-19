#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_HOME/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start
#$HADOOP_PREFIX/sbin/start-dfs.sh

if [ $ISMASTER -eq 1 ]
then
    /sabd/start-services.sh
    yarn nodemanager
fi

# Launch bash console  
/bin/bash
