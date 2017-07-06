#!/bin/bash


source /etc/profile
cd `dirname $0`
pwd=`pwd`
echo $pwd

ARGS=`getopt -o d:t:s:e:h --long database:,table:,startDate:,endDate:,hour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -d|--database)
            database=$2;
            shift 2;;
		-t|--table)
            table=$2;
            shift 2;;
        -s|--startDate)
            startDate=$2;
            shift 2;;
        -e|--endDate)
            endDate=$2;
            shift 2;;
        -h|--hour)
            hour=$2;
            shift 2;;
        --)
            shift;
            break;;

        *)
            exit 1
            ;;
    esac
done


hour_p=$hour

day_p=`date -d "-1 days "$startDate +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`


echo "table is $table"
echo "day_p is $day_p"
echo "endDate is $endDate"
echo "hour_p is $hour_p"

while [[ $day_p -le $endDate ]]
 do
  echo "$day_p    ..................."
  hive  -hivevar table=$table -hivevar day_p=$day_p -hivevar hour_p=$hour_p  -e '
  use dw_facts;
  alter table ${hivevar:table} add if not exists partition (day_p=${hivevar:day_p},hour_p="${hivevar:hour_p}") location "/data_warehouse/dw_facts/${hivevar:table}/${hivevar:day_p}/${hivevar:hour_p}";
  '
  if [ $? -ne 0 ];then
   echo "hive addPartitions  ${day_p} is fail ..."
   exit 1
  fi
  day_p=`date -d "+1 days "$day_p +%Y%m%d`
done

