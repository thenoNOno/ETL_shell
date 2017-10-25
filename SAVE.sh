#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: SAVE.sh
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-05-25
#	Description: save模块
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#脚本变量配置--------------------------------------------------------------
ETL_PATH="$1"

#SAVESQL变量配置--------------------------------------------------------------
summarytable="$2"
longbreakdowntable="$3"
breakdowntable="$4"
EDWsummarytable="$5"
EDWlongbreakdowntable="$6"
EDWbreakdowntable="$7"
LOG_TIME="'$8'"
partition="$9"
#-------------------------------------------------------------------------

#######################ETL模块############################
#  Step 1. 制作SAVESQL
SAVEsummarytable="insert into table $EDWsummarytable PARTITION (day, channel) SELECT pid,originalpid,author,authorid,posttime,title,reply,createtime,etltime,channel,$partition as day FROM $summarytable "
SAVElongbreakdowntable="insert into table $EDWlongbreakdowntable PARTITION (day, channel) SELECT longbreakdownid,longbreakdowncontent,pos,originalpid,posttime,etltime,channel,$partition as day FROM $longbreakdowntable "
SAVEbreakdowntable="insert into table $EDWbreakdowntable PARTITION (day, channel) SELECT breakdownid,breakdowncontent,pos,longbreakdownid,originalpid,posttime,etltime,channel,$partition as day FROM $breakdowntable "
#  Step 2. 执行SAVESQL
sh -x $ETL_PATH/execute_save.sql "$SAVEsummarytable" "$SAVElongbreakdowntable" "$SAVEbreakdowntable" 


