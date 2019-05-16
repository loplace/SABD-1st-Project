FROM harisekhon/hbase:1.4

ADD hbase-conf/hbase-site.xml /hbase/conf
ADD hbase-conf/create_tables /create_tables
ADD hbase-conf/start-hbase.sh /start-hbase.sh
RUN chmod +x /start-hbase.sh


# HBASE ports
EXPOSE 2181 8080 8085 9090 9095 16000 16010 16201 16301


#CMD "/entrypoint.sh"
CMD "/bin/bash"