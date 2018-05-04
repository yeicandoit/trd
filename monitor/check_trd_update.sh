#!/bin/bash

app='applog'
mtype='update'
log_path="/opt/trd/applog/log/trd.log"
log_stamp=`date +%s -r $log_path`
now_stamp=`date +%s`

if [ $now_stamp -gt $log_stamp ];
then
    status=2
    statustxt="applog of trd may have some error"
else 
    status=0
    statustxt="applog of trd is OK"
fi

echo "$status check_${app}_$mtype num=$status;0;1; $statustxt"

if [ $status -eq 2 ];
then
    mail="python /opt/trd/monitor/myEmail.py"
    hostname=`hostname`
    to="wangqiang@optaim.com"
    subject="applog server monitor"
    body="$hostname $app $mtype may have error"
    $mail -t "$to" -s "$subject" -b "$body"
fi
