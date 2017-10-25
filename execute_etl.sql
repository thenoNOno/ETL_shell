#!/bin/sh

/usr/local/spark/bin/spark-shell --executor-memory 5G --driver-class-path /usr/local/hive/lib/hive-hbase-handler-2.1.0.jar:/usr/local/hive/lib/zookeeper-3.4.6.jar:/usr/local/hive/lib/hbase-common-1.1.1.jar:/usr/local/hive/lib/hbase-client-1.1.1.jar:/usr/local/hive/lib/hbase-hadoop2-compat-1.1.1.jar:/usr/local/hive/lib/hbase-server-1.1.1.jar:/usr/local/hive/lib/hbase-protocol-1.1.1.jar:/usr/local/hive/lib/htrace-core-3.1.0-incubating.jar:/usr/local/hive/lib/commons-collections-3.2.2.jar:/usr/local/hive/lib/guava-14.0.1.jar --jars /usr/local/hive/lib/hive-hbase-handler-2.1.0.jar,/usr/local/hive/lib/zookeeper-3.4.6.jar,/usr/local/hive/lib/hbase-common-1.1.1.jar,/usr/local/hive/lib/hbase-client-1.1.1.jar,/usr/local/hive/lib/hbase-hadoop2-compat-1.1.1.jar,/usr/local/hive/lib/hbase-server-1.1.1.jar,/usr/local/hive/lib/hbase-protocol-1.1.1.jar,/usr/local/hive/lib/htrace-core-3.1.0-incubating.jar,/usr/local/hive/lib/commons-collections-3.2.2.jar,/usr/local/hive/lib/guava-14.0.1.jar<<EOF

sqlContext.sql("set hive.enforce.bucketing = true").show
sqlContext.sql("set mapred.reduce.tasks=6").show

val temptable = sqlContext.sql("$1")
temptable.registerTempTable("temptable")
sqlContext.cacheTable("temptable")
sqlContext.uncacheTable("temptable")

sqlContext.sql("$2").show

sqlContext.sql("$3").show

EOF
stty echo
