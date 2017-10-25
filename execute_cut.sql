#!/bin/sh
/usr/local/spark/bin/spark-shell --executor-memory 5G --total-executor-cores 3  --master spark://skn-7qm6ztfc-spark-master:7077  --jars /usr/local/spark/lib/sqljdbc4.jar --driver-class-path /usr/local/spark/lib/sqljdbc4.jar<<EOF

val Long_Split_TermList = List("…","...","!","！","?","？","。","；",";")
val Short_Split_TermList = List("，",",","(","（",")","）")

sqlContext.udf.register("UDFLongbreakdown", (s: String) =>   {var s1 = s ;for (i <- Long_Split_TermList) s1 = s1.replace(i,i+"\t"); s1.split("\t").filter( x => {var b = 1;for (i <- Long_Split_TermList) if (x == i) {b = 0};b ==1})})

sqlContext.udf.register("UDFbreakdown", (s: String) =>   {var s1 = s ;for (i <- Short_Split_TermList) s1 = s1.replace(i,i+"\t"); s1.split("\t").filter( x => {var b = 1;for (i <- Short_Split_TermList) if (x == i) {b = 0};b ==1})})

sqlContext.sql("set hive.enforce.bucketing = true").show

val temptable = sqlContext.sql("$1")
temptable.registerTempTable("temptable")
sqlContext.cacheTable("temptable")
sqlContext.uncacheTable("temptable")

sqlContext.sql("insert overwrite table $2 select LID,Content,pos,originalpid,posttime,channel,etltime from (select concat(originalpid,'_L',row_number() over(partition by originalpid)) as LID,Content,row_number() over(partition by originalpid) as pos,originalpid,posttime,channel,etltime from (select originalpid,explode(UDFLongbreakdown(if(floorlevel=0,concat(COALESCE(concat(case when isnull(title) then '' else title end,'。'),''),case when isnull(reply) then '' else reply end),case when isnull(reply) then '' else reply end))) as content,posttime,channel,etltime from temptable )t where trim(content)<>'')t").show


sqlContext.sql("insert overwrite table $3 select SID,ShortContent,pos,Longbreakdownid,originalpid,posttime,channel,etltime from (select concat(longbreakdownid,'_S',row_number() over(partition by longbreakdownid)) as SID,ShortContent,row_number() over(partition by longbreakdownid) as pos,Longbreakdownid,originalpid,posttime,channel,etltime from (select longbreakdownid,originalpid,explode(UDFbreakdown(longbreakdowncontent)) as ShortContent,posttime,channel,etltime from $2 ) t where trim(ShortContent)<>'')t").show

EOF
stty echo
