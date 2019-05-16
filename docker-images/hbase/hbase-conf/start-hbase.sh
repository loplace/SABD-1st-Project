#!/bin/bash
/entrypoint.sh
$HBASE_HOME/bin/hbase shell < /create_tables