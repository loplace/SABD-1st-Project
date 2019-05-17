FROM harisekhon/hbase:1.4

ENV HBASE_HOME /hbase

ADD hbase-conf/hbase-site.xml $HBASE_HOME/conf

ADD hbase-conf/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ADD hbase-conf/create_tables /create_tables
RUN chmod 755 /create_tables


# HBASE ports
EXPOSE 2181 8080 8085 9090 9095 16000 16010 16201 16301


CMD "/entrypoint.sh"