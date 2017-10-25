#!/bin/sh

#
#-------------------CopyRight--------------------------
#	FileName: AUTO_ETL
#	Version: Number:1.00
#	Author: qixuan
#	Date: 2017-05-25
#	Description: AUTO_ETL
#-------------------Execution Description--------------
#	DataOutDir: 
#	DataInputDir:
#------------------------------------------------------

#log变量配置--------------------------------------------------------------
SPARK_LOGPATH="$1"
SH_LOGPATH="$2"
LOG_DATE="$3"
LOG_TIMESTAMP="$4"
ETL_TIME=$LOG_DATE" "$LOG_TIMESTAMP
LOG_TIME=$LOG_DATE":"$LOG_TIMESTAMP
SPARK_LOGFILE="$SPARK_LOGPATH/lg_auto_etl_$LOG_TIME.log"
SH_LOGFILE="$SH_LOGPATH/lg_auto_etl_$LOG_TIME.log"
#-------------------------------------------------------------------------
#ETL变量配置--------------------------------------------------------------
ETL_PATH="$5"
sourcetable="$6"
summarytable="$7"
longbreakdowntable="$8"
breakdowntable="$9"
EDWsummarytable="$10"
EDWlongbreakdowntable="$11"
EDWbreakdowntable="$12"
partition="$13"
#获取lasttime
lasttimefile="$ETL_PATH/"$EDWsummarytable"_lasttime.log"
if [ ! -x "$lasttimefile" ];
then echo "1970-01-01 00:00:00" > $lasttimefile;
fi
lasttime=`sed -n '1p' $lasttimefile`
#-------------------------------------------------------------------------

#######################AUTO_ETL############################

echo " -------------------------------------------------------------------- " >> $SH_LOGFILE

#Step1 hbase->staging

echo "`date "+%Y-%m-%d %H:%M:%S"` Step.1 hbase->staging started." >> $SH_LOGFILE
cd $ETL_PATH
sh -x ./ETL.sh "$ETL_PATH" "$ETL_TIME" "$lasttime" "$sourcetable" "$summarytable" "$EDWsummarytable" >> $SPARK_LOGFILE

fail_cnt=`grep failed $SPARK_LOGFILE|wc -l`
exception_cnt=`grep Exception $SPARK_LOGFILE|wc -l`
aborted_cnt=`grep aborted $SPARK_LOGFILE|wc -l`

error_cnt=`expr $fail_cnt + $exception_cnt + $aborted_cnt`
echo $error_cnt
if [ $error_cnt -gt 0 ];then
echo "`date "+%Y-%m-%d %H:%M:%S"` hbase->staging failed." >> $SH_LOGFILE
echo "`date "+%Y-%m-%d %H:%M:%S"` job aborted." >> $SH_LOGFILE
exit
else
echo "`date "+%Y-%m-%d %H:%M:%S"` hbase->staging completed." >> $SH_LOGFILE
fi

echo " -------------------------------------------------------------------- " >> $SH_LOGFILE

#Step2 staing->staging longbreakdown breakdown

echo "`date "+%Y-%m-%d %H:%M:%S"` Step.2 staging->staging longbreakdown&breakdown started." >> $SH_LOGFILE
cd $ETL_PATH
sh -x ./CUT.sh "$ETL_PATH" "$longbreakdowntable" "$breakdowntable" "$summarytable" "$EDWsummarytable" >> $SPARK_LOGFILE


fail_cnt=`grep failed $SPARK_LOGFILE|wc -l`
exception_cnt=`grep Exception $SPARK_LOGFILE|wc -l`
aborted_cnt=`grep aborted $SPARK_LOGFILE|wc -l`

error_cnt=`expr $fail_cnt + $exception_cnt + $aborted_cnt`
echo $error_cnt
if [ $error_cnt -gt 0 ];then
echo "`date "+%Y-%m-%d %H:%M:%S"` staging->staging longbreakdown&breakdown failed." >> $SH_LOGFILE
echo "`date "+%Y-%m-%d %H:%M:%S"` job aborted." >> $SH_LOGFILE
exit
else
echo "`date "+%Y-%m-%d %H:%M:%S"` staging->staging longbreakdown&breakdown completed." >> $SH_LOGFILE
fi

echo " -------------------------------------------------------------------- " >> $SH_LOGFILE

#Step3 staing->EDW ALL

echo "`date "+%Y-%m-%d %H:%M:%S"` Step.3 staing->EDW ALL started." >> $SH_LOGFILE
cd $ETL_PATH
sh -x ./SAVE.sh "$ETL_PATH" "$summarytable" "$longbreakdowntable" "$breakdowntable" "$EDWsummarytable" "$EDWlongbreakdowntable" "$EDWbreakdowntable" "$ETL_TIME" "$partition">> $SPARK_LOGFILE

fail_cnt=`grep failed $SPARK_LOGFILE|wc -l`
exception_cnt=`grep Exception $SPARK_LOGFILE|wc -l`
aborted_cnt=`grep aborted $SPARK_LOGFILE|wc -l`

error_cnt=`expr $fail_cnt + $exception_cnt + $aborted_cnt`
echo $error_cnt
if [ $error_cnt -gt 0 ];then
echo "`date "+%Y-%m-%d %H:%M:%S"` staing->EDW ALL failed." >> $SH_LOGFILE
echo "`date "+%Y-%m-%d %H:%M:%S"` job aborted." >> $SH_LOGFILE
exit
else
echo "`date "+%Y-%m-%d %H:%M:%S"` staing->EDW ALL completed." >> $SH_LOGFILE
fi

echo " -------------------------------------------------------------------- " >> $SH_LOGFILE

#Step4 check&clean
echo "`date "+%Y-%m-%d %H:%M:%S"` Step.4 check&clean started." >> $SH_LOGFILE
cd $ETL_PATH
sh -x ./LAST.sh "$ETL_PATH" "$summarytable" "$longbreakdowntable" "$breakdowntable"  >> $SH_LOGFILE
echo "`date "+%Y-%m-%d %H:%M:%S"` Step.4 check&clean completed." >> $SH_LOGFILE

echo " -------------------------------------------------------------------- " >> $SH_LOGFILE

#Step5 updatelasttime
echo "`date "+%Y-%m-%d %H:%M:%S"` Step.5 updatelasttime." >> $SH_LOGFILE
echo "$ETL_TIME" > $lasttimefile
echo LASTTIME: "$lasttime" >> $SH_LOGFILE
echo ETLTIME: "$ETL_TIME" >> $SH_LOGFILE
