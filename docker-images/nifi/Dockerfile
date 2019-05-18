FROM ubuntu:xenial
USER root

RUN apt-get update
RUN apt-get install -y wget openjdk-8-jre

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64


#APACHE NIFI
RUN wget http://it.apache.contactlab.it/nifi/1.9.2/nifi-1.9.2-bin.tar.gz ; tar -zxf nifi-1.9.2-bin.tar.gz -C /usr/local/ ; rm nifi-1.9.2-bin.tar.gz
RUN cd /usr/local && ln -s ./nifi-1.9.2 nifi
ENV NIFI_HOME /usr/local/nifi
RUN cd $NIFI_HOME/conf; mv nifi.properties nifi.properties-old;

ADD config/nifi.properties $NIFI_HOME/conf
ADD config/bootstrap.conf $NIFI_HOME/conf
#ADD config/flow.xml.gz $NIFI_HOME/conf

VOLUME $NIFI_HOME/conf/flow
VOLUME $NIFI_HOME/extensions

RUN mkdir /data; mkdir /data/hdfsconf; mkdir /data/hbaseconf
ADD hdfs-config/core-site.xml /data/hdfsconf
ADD hdfs-config/hdfs-site.xml /data/hdfsconf
ADD hbase-config/hbase-site.xml /data/hbaseconf/

ENV DATASET_REPO http://www.acsiciclismoabruzzo.it/sabd/
WORKDIR /data
RUN wget $DATASET_REPO/city_attributes.csv; wget $DATASET_REPO/temperature.csv; wget $DATASET_REPO/weather_description.csv; wget $DATASET_REPO/pressure.csv; wget $DATASET_REPO/humidity.csv; 

# trucco per tenere attivo il container dopo lo start
# exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
CMD $NIFI_HOME/bin/nifi.sh start ; exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

EXPOSE 5555 

# nifi remote debugging
EXPOSE 43210
