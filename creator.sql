cd /usr/local/spark
bie/spark-shell --executor-memory 12G --master spark://skn-7qm6ztfc-spark-master:7077 --jars /usr/local/spark/lib/sqljdbc4.jar --driver-class-path /usr/local/spark/lib/sqljdbc4.jar

summarytable="$1"
longbreakdowntable="$2"
breakdowntable="$3"

sql("drop table if exists $summarytable").show
sql("drop table if exists $longbreakdowntable").show
sql("drop table if exists $breakdowntable").show
sql("create table if not exists $summarytable(pid string,originalpid string,channel string,author string,authorid string,posttime string,title string,reply string,createtime string,etltime TIMESTAMP)row format DELIMITED FIELDS TERMINATED BY '\001' NULL DEFINED AS '' stored as orcfile;").show
sql("create table if not exists $longbreakdowntable(longbreakdownid string,longbreakdowncontent string,pos int,originalpid string,posttime string,channel string,etltime TIMESTAMP) row format DELIMITED FIELDS TERMINATED BY '\001' NULL DEFINED AS '' stored as orcfile;").show
sql("create table if not exists $breakdowntable(breakdownid string,breakdowncontent string,pos int,longbreakdownid string,originalpid string,posttime string,channel string,etltime TIMESTAMP)row format DELIMITED FIELDS TERMINATED BY '\001' NULL DEFINED AS '' stored as orcfile;").show



