# Creates pseudo distributed hadoop 3.1.2
#
# docker build -t effeerre/hadoop .

FROM ubuntu:xenial
USER root

# dipendenze di timezonefinder
RUN apt-get update
RUN apt-get install -y wget openjdk-8-jre

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64


#APACHE NIFI
RUN wget http://it.apache.contactlab.it/nifi/1.9.2/nifi-1.9.2-bin.tar.gz
RUN tar -zxf nifi-1.9.2-bin.tar.gz -C /usr/local/ 
RUN rm nifi-1.9.2-bin.tar.gz

RUN cd /usr/local && ln -s ./nifi-1.9.2 nifi
ENV NIFI_HOME /usr/local/nifi
RUN cd $NIFI_HOME/conf; mv nifi.properties nifi.properties-old;
ADD config/nifi.properties $NIFI_HOME/conf
ADD config/flow.xml.gz $NIFI_HOME/conf


CMD $NIFI_HOME/bin/nifi.sh start

EXPOSE 5555
