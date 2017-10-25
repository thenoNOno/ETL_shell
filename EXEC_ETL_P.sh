#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: EXEC_ETL_P
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-05-25
#	Description: EXEC_ETL_P
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#接收参数-----------------------------------------------------------------
schema="$1"
partition="$2"

#log变量配置--------------------------------------------------------------
SPARK_LOGPATH="/usr/local/colourdata/etl/logs/sparkshell"
SH_LOGPATH="/usr/local/colourdata/etl/logs/shell"
LOG_DATE=`date "+%Y-%m-%d"`
LOG_TIMESTAMP=`date "+%H:%M:%S"`

#ETL变量配置--------------------------------------------------------------
ETL_PATH="/usr/local/colourdata/etl/autoetl"
sourcetable="datalake.auto_summary_hbase_external"
summarytable="$schema.summary_staging"
longbreakdowntable="$schema.summary_longbreakdown_staging"
breakdowntable="$schema.summary_breakdown_staging"
EDWsummarytable="$schema.summary_EDW"
EDWlongbreakdowntable="$schema.summary_longbreakdown_EDW"
EDWbreakdowntable="$schema.summary_breakdown_EDW"
#partition="to_date(posttime)"
#-------------------------------------------------------------------------
#执行AUTO_ETL
cd $ETL_PATH
./AUTO_ETL.sh "$SPARK_LOGPATH" "$SH_LOGPATH" "$LOG_DATE" "$LOG_TIMESTAMP" "$ETL_PATH" "$sourcetable" "$summarytable" "$longbreakdowntable" "$breakdowntable" "$EDWsummarytable" "$EDWlongbreakdowntable" "$EDWbreakdowntable" "$partition" 

echo $LOG_DATE" "$LOG_TIMESTAMP
