# Creates pseudo distributed hadoop 3.1.2
#
# docker build -t effeerre/hadoop .

FROM effeerre/hadoop
USER root

RUN apt-get update;
RUN apt-get install nano;

# HDFS CONFIG FILES
ADD hdfs-config/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh
ADD hdfs-config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD hdfs-config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD hdfs-config/workers $HADOOP_HOME/etc/hadoop/workers

ADD hdfs-config/bootstrap.sh /usr/local/bootstrap.sh
RUN chown root:root /usr/local/bootstrap.sh
RUN chmod 700 /usr/local/bootstrap.sh

# SPARK LIB FILES
RUN wget http://it.apache.contactlab.it/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz; 
RUN tar -zxf spark-2.4.3-bin-hadoop2.7.tgz -C /usr/local/ ; rm spark-2.4.3-bin-hadoop2.7.tgz
RUN cd /usr/local && ln -s ./spark-2.4.3-bin-hadoop2.7 spark
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin
RUN mkdir $SPARK_HOME/yarn-remote-client

# YARN CONFIG FILES
ADD yarn-config/yarn-remote-client/core-site.xml $SPARK_HOME/external/spark-native-yarn/conf/
ADD yarn-config/yarn-remote-client/yarn-site.xml $SPARK_HOME/external/spark-native-yarn/conf/
ADD yarn-config/yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

# SPARK CONFIG FILES
ADD spark-config/spark-env.sh $SPARK_HOME/conf
ADD spark-config/spark-defaults.conf $SPARK_HOME/conf
ADD spark-config/slaves $SPARK_HOME/conf

# BOOTSTRAP SERVICE SCRIPT
RUN mkdir /sabd
ADD start-services.sh /sabd
RUN chmod 700 /sabd/start-services.sh
ADD stop-services.sh /sabd
RUN chmod 700 /sabd/stop-services.sh

ENV PATH $PATH:$SPARK_HOME/bin:$HADOOP_PREFIX/bin

ENV BOOTSTRAP /usr/local/bootstrap.sh
CMD /usr/local/bootstrap.sh

# # HDFS ports
EXPOSE 9866 9867 9870 9864 9868 9820 9000
EXPOSE 54310
# # Mapred ports
# EXPOSE 10020 19888

# YARN ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088

# #Other ports
# EXPOSE 49707 2122
#
# SSH port
EXPOSE 22
