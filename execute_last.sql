#!/bin/sh

/usr/local/spark/bin/spark-shell --executor-memory 5G --total-executor-cores 3  --master spark://skn-7qm6ztfc-spark-master:7077  --jars /usr/local/spark/lib/sqljdbc4.jar --driver-class-path /usr/local/spark/lib/sqljdbc4.jar<<EOF
sqlContext.sql("$1").show
sqlContext.sql("$2").show
sqlContext.sql("$3").show
sqlContext.sql("$4")
sqlContext.sql("$5")
sqlContext.sql("$6")
EOF
stty echo
