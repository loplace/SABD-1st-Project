# SABD-1st-Project
Repo for SABD course project year 2018/2019

Tiro su l'ambiente con 1 master e 3 slave

1. `docker-compose up -d`

Login in bash masternode

2. `docker exec -it master bash`

SOLO PRIMO AVVIO!!

3. `hdfs namenode -format`

Avvio HDFS

4. `$HADOOP_HOME/sbin/start-dfs.sh`

Controllo corretto avvio HDFS

5. `hdfs dfsadmin -report`

oppure al link http://localhost:9870/dfshealth.html

Termino HDFS

6. `$HADOOP_HOME/sbin/stop-dfs.sh`
