#!/bin/sh

/usr/local/spark/bin/spark-shell --executor-memory 5G --total-executor-cores 3  --master spark://skn-7qm6ztfc-spark-master:7077  --jars /usr/local/spark/lib/sqljdbc4.jar --driver-class-path /usr/local/spark/lib/sqljdbc4.jar<<EOF

sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict").show
sqlContext.sql("set hive.exec.max.dynamic.partitions.pernode=36600").show
sqlContext.sql("set hive.enforce.bucketing = true").show
sqlContext.sql("set mapred.reduce.tasks=6").show

sqlContext.sql("$1").show

sqlContext.sql("$2").show

sqlContext.sql("$3").show


EOF
stty echo
