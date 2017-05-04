#!/bin/bash
if [ $# -ne 8 ]; then
    echo " usage : `basename  $0`  8 个 参数 '--table ${table} --startDate ${startDate} --endDate ${endDate} --hour ${hour} '"
    exit 1
fi

source /etc/profile
cd `dirname $0`
pwd=`pwd`
echo $pwd
table=$2
day_p=$4
endDate=$6
hour_p=$8
echo $day_p
echo $hour_p
day_p=`date -d "-1 days "$day_p +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`
echo "table is $table"
echo "day_p is $day_p"
echo "endDate is $endDate"
echo "hour_p is $hour_p"

while [[ $day_p -le $endDate ]]
 do
  echo "$day_p    ..................."
  hive -hivevar table=$table -hivevar day_p=$day_p -hivevar hour_p=$hour_p  -e '
  use dw_facts;
  alter table ${hivevar:table} add if not exists partition (day_p=${hivevar:day_p},hour_p="${hivevar:hour_p}") location "/data_warehouse/dw_facts/${hivevar:table}/${hivevar:day_p}/${hivevar:hour_p}";
  '
  day_p=`date -d "+1 days "$day_p +%Y%m%d`
done

