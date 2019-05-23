#!/bin/bash
PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME -q <queryname> -e <context> -d <fileformat>

-q : name of query [query1|query2|query3]
-e : execution context for spark [local|yarn]
-d : format of dataset in input [csv|parquet|kafka]

EOF
  exit 1
}
# valori di default
#queryname=query1 context=local fileformat=csv

while getopts q:e:d:r o; do
  case $o in
    (q) queryname=$OPTARG;;
    (e) context=$OPTARG;;
    (d) fileformat=$OPTARG;;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

#echo Remaining arguments: "$@"
wrong_query_name() {
    echo "ERROR: Query name not exists";
    usage
}
#set query class name
query_class="queries.FirstQuerySolver"
case $queryname in
    ("query1") query_class="queries.FirstQuerySolver";;
    ("query2") query_class="queries.SecondQuerySolver";;
    ("query3") query_class="queries.ThirdQuerySolver";;
    (*) wrong_query_name
esac

echo "queryname: "$queryname;
echo "context: "$context;
echo "fileformat: "$fileformat;
echo "query_class: "$query_class;

spark_submit_local() {
  $SPARK_HOME/bin/spark-submit \
    --class $query_class \
    --master $context \
    /sabd/jar/SABD-project1-1.0-SNAPSHOT.jar $context $fileformat $queryname

}
spark_submit_cluster() {

  $SPARK_HOME/bin/spark-submit \
    --class $query_class \
    --master $context \
    --deploy-mode cluster \
    --driver-memory 2g \
    --executor-memory 1g \
    --executor-cores 1 \
    /sabd/jar/SABD-project1-1.0-SNAPSHOT.jar $context $fileformat $queryname

}

if [ $context = "yarn" ]; then
  spark_submit_cluster
else 
  spark_submit_local
fi