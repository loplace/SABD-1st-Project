# SABD-1st-Project
Repo for SABD course project year 2018/2019

Tiro su l'ambiente con 1 master e 3 slave

1. `docker-compose up -d`

Login in bash masternode

2. `docker exec -it master bash`

Avvio dei servizi (HDFS - YARN - SPARK)

3. `/sabd/start-services.sh`

Controllo corretto avvio HDFS

4. `hdfs dfsadmin -report`

oppure al link http://localhost:9870/dfshealth.html

Termino i serivizi (HDFS - YARN - SPARK)

5. `/sabd/stop-services.sh`
