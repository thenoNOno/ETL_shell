#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: ETL
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-06-16
#	Description: LAST模块
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#脚本变量配置--------------------------------------------------------------
ETL_PATH="$1"

#CHECKSQL变量配置--------------------------------------------------------------
summarytable="$2"
longbreakdowntable="$3"
breakdowntable="$4"

#-------------------------------------------------------------------------

#######################LAST模块############################
#  Step 1. 制作CHECKSQL&CLEANSQL
CHECKSQL1="select count(*) as summary from $summarytable"
CHECKSQL2="select count(*) as longbreakdown from $longbreakdowntable"
CHECKSQL3="select count(*) as breakdown from $breakdowntable"
CLEANSQL1="truncate table $summarytable"
CLEANSQL2="truncate table $longbreakdowntable"
CLEANSQL3="truncate table $breakdowntable"
#  Step 2. 执行CHECKSQL&CLEANSQL
sh -x $ETL_PATH/execute_last.sql "$CHECKSQL1" "$CHECKSQL2" "$CHECKSQL3" "$CLEANSQL1" "$CLEANSQL2" "$CLEANSQL3" 


