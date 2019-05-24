# SABD-1st-Project
Repo for SABD course project year 2018/2019.

Design and implement a big data batch system for compute queries on meteorology dataset 

Environment
------
Environment is composed by 10 docker containers:
* 3x hdfs-spark-yarn slaves
* 1x hdfs-spark-yarn master server
* 1x hbase server
* 1x kafka message broker
* 1x zookeeper
* 1x jetty web server
* 1x citylocationhelper
* 1x nifi
 
- Jetty Web Server is used to run the SABD GUI, a dashboard for visualize queries results
- citylocationhelper is a custom container that wraps 2 python libraries for timezone and geoinformation retrieving on a tcp socket

To start the environment
------
The environment can be managed by docker-compose. To start, type the following command in docker-compose folder
```console
docker-compose up -d --build
```

All containers start automatically, wait few minutes.

### Phase 1: Data ingestion

Go to Nifi Dashboard (http://localhost:5555/nifi/) and start the following processors groups:
- <b>retrieve location infos</b>: for retrieve timezone and country information
- <b>CSVtoHDFS</b>
- CSVtoPasquet 	[optional]
- CSVtoKafka	[optional]

### Phase 2: Execution

When data ingestion phase is completed, type following commands to run queries
```console
docker-exec -it master bash
```
then
```console
jar/submit-query.sh
```

Usage of submit-query.sh:
```console
Usage: submit-query.sh -q <queryname> -e <context> -d <fileformat>

-q : name of query [query1|query2|query3]
-e : execution context for spark [local|yarn]
-d : format of dataset in input [csv|parquet|kafka]
```

For example 
```console
submit-query.sh -q query1 -e local -d csv 
```
to start the computation of query1 in local machine with csv format for the dataset

The available execution environments are:
* <b>local</b>: spark job is executed only on master container
* <b>yarn</b>: spark job is executed on a cluster composed by 4 container


For monitoring cluster, YARN dashboard is available at http://localhost:8088/cluster

Spark execution history is available at http://localhost:18080

Queries results is saved on output HDFS folder, (http://localhost:9870/explorer.html#/output)

### Phase 3: Upload and visualize queries results

Go back to NiFi and start processors group named:
- Save results on hbase

Results are available at SABD-GUI available at http://localhost:65000/sabdgui/

## Owners

Ronci Federico - Computer Engineering - University of Rome Tor Vergata

Schiazza Antonio - Computer Engineering - University of Rome Tor Vergata

