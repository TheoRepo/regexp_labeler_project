#!/usr/bin/bash

start_date=$1
end_date=$2

function list_date {
    start=$1
    end=$2
    # 计算从开始的日期到结束的日期间隔了几天
    length=$((($(date +%s -d "${end}") - $(date +%s -d "${start}"))/86400));
    dates=""
    while [ $length -gt 0 ];
    do
        d=`date -d "$end -$length day" +%Y-%m-%d`
        # 在字符串结尾增加内容
        dates=${dates}" ${d}"
        # 循环指标 n-1
        let length=`expr $length - 1`
    done
    echo ${dates: 1}
}

# 前开后闭
dates=`list_date $start_date $end_date`
echo $dates
for d in $dates
do
    echo "$d 数据开始跑数"
    sh run.sh --config ./config/config.toml --the_date $d --file_no all
    # $!是Shell最后运行的后台Process的PID
    if [[ $! -eq 0 ]]
    then
        echo "$d 数据跑数结束"
    else
        exit
    fi
done