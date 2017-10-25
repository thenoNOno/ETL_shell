#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: CUT.sh
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-06-08
#	Description: CUT模块
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#脚本变量配置--------------------------------------------------------------
ETL_PATH="$1"

#SAVESQL变量配置--------------------------------------------------------------
longbreakdowntable="$2"
breakdowntable="$3"
summarytable="$4"
EDWsummarytable="$5"
#-------------------------------------------------------------------------

#######################CUT模块############################
#  Step 1. 制作CUTSQL
#本批次去重
TEMPSQL="select t.* from (SELECT originalpid,channel,title,reply,posttime,etltime,row_number() over(partition by concat(originalpid,channel)) as rank FROM $summarytable) t where t.rank=1" 
#与全量表匹配去重
CUTSQL="select a.* from ($TEMPSQL) a left outer join $EDWsummarytable b on a.originalpid=b.originalpid and a.channel=b.channel where b.originalpid is null "
#  Step 2. 执行SAVESQL
sh -x $ETL_PATH/execute_cut.sql "$CUTSQL" "$longbreakdowntable" "$breakdowntable" 

echo `date "+%Y-%m-%d"` `date "+%H:%M:%S"`
