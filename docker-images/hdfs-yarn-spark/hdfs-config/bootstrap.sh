#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start
#$HADOOP_PREFIX/sbin/start-dfs.sh

if [ $ISMASTER -eq 1 ]
then
    /sabd/start-services.sh
fi

# Launch bash console  
/bin/bash
