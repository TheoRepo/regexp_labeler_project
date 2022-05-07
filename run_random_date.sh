#!/usr/bin/bash

num_date=$1

function find_rand_date {
    n=$1
    start=$2
    end=$3
    length=$((($(date +%s -d "${end}") - $(date +%s -d "${start}"))/86400));
    dates=""
    while [ $n -gt 0 ];
    do
        num=$((RANDOM%$length))
        d=`date -d "$end -$num day" +%Y-%m-%d`
        dates=${dates}" ${d}"
        let n=`expr $n - 1`
    done
    echo ${dates: 1}
}

dates=`find_rand_date $num_date 20200101 20220101`
echo $dates
for d in $dates
do
    echo "$d 数据开始跑数"
    sh run.sh --config ./config/config.toml --the_date $d --file_no all
    if [[ $! -eq 0 ]]
    then
        echo "$d 数据开始跑数"
    else
        exit
    fi
done