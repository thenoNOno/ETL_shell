#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: ETL
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-05-25
#	Description: etl模块
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#脚本变量配置--------------------------------------------------------------
ETL_PATH="$1"
etltime="'$2'"
lasttime="'$3'"
sourcetable="$4"
summarytable="$5"
EDWsummarytable="$6"
#ETLSQL变量配置--------------------------------------------------------------
pid="pid"
originalpid="originalpid"
channel="channel"
author="case when isnull(author) or author='' then null else trim(author) end"
authorid="case when isnull(authorid) or authorid='' then null else trim(authorid) end"
posttime="COALESCE(regexp_replace(posttime,'/','-'),'1970-01-01')" 
title="case when isnull(title) then null else trim(title) end"
reply="case when isnull(reply) then null else trim(reply) end"
createtime="createtime"
#新增字段,同时需要ETLSQL中添加$image变量
#image="image"
#-------------------------------------------------------------------------

#######################ETL模块############################
#  Step 1. 制作ETLSQL
TEMPSQL="select a.* from $sourcetable a left outer join $EDWsummarytable b on a.pid=b.pid where b.pid is null and (a.createtime>=$lasttime and a.createtime<$etltime)"
ETLSQL="insert overwrite table $summarytable SELECT $pid,$originalpid,$channel,$author,$authorid,$posttime,$title,$reply,$createtime,$image,$etltime FROM temptable "

#  Step 2. 执行ETLSQL
#truncate临时表&执行ETL
sh -x $ETL_PATH/execute_etl.sql "$TEMPSQL" "$ETLSQL" "select count(*) from $summarytable"


