#!/bin/bash
cd `dirname $0`
pwd=`pwd`
echo $pwd
table=$2
day_p=$4
hour_p=$6
echo $day_p
echo $hour_p
day_p=`date -d "-1 days "$day_p +%Y%m%d`
echo "table is $table"
echo "day_p is $day_p"
echo "hour_p is $hour_p"
hive -hivevar table=$table -hivevar day_p=$day_p -hivevar hour_p=$hour_p  -e '
use dw_facts;
alter table ${hivevar:table} add if not exists partition (day_p=${hivevar:day_p},hour_p="${hivevar:hour_p}") location "/data_warehouse/dw_facts/${hivevar:table}/${hivevar:day_p}/${hivevar:hour_p}";
'