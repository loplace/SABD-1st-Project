#!/bin/bash
#export UPLOAD_DEBUG=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=0.0.0.0:43212
export UPLOAD_DEBUG=

# UPLOAD RESULTS QUERY1
cat results/query1-part-00000 | \
java $UPLOAD_DEBUG -jar results-uploader-1.0-SNAPSHOT.jar query1 cities true


# UPLOAD RESULTS QUERY2
cat results/query2-humidities-part-00000 | \
java $UPLOAD_DEBUG -jar results-uploader-1.0-SNAPSHOT.jar query2 country_info true

cat results/query2-humidities-part-00000 | \
java $UPLOAD_DEBUG -jar results-uploader-1.0-SNAPSHOT.jar query2 humidity false

cat results/query2-temperatures-part-00000 | \
java $UPLOAD_DEBUG -jar results-uploader-1.0-SNAPSHOT.jar query2 temperature false

cat results/query2-pressures-part-00000 | \
java $UPLOAD_DEBUG -jar results-uploader-1.0-SNAPSHOT.jar query2 pressure false

# UPLOAD RESULTS QUERY3
cat results/query3-part-00000 | \
java $UPLOAD_DEBUG -jar results-uploader-1.0-SNAPSHOT.jar query3 country_city true

