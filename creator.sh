#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: creator.sh
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-06-08
#	Description: creator
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#脚本变量配置--------------------------------------------------------------
schema="$1"
ETL_PATH="$2"

#SAVESQL变量配置--------------------------------------------------------------
summarytable="$schema.auto_summary"
longbreakdowntable="$schema.auto_summary_longbreakdown"
breakdowntable="$schema.auto_summary_breakdown"
#-------------------------------------------------------------------------

#######################CUT模块############################
#Step 1. creator
sh -x $ETL_PATH/creator.sql "$summarytable" "$longbreakdowntable" "$breakdowntable" 


