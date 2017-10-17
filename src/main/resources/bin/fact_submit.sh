#!/bin/bash

startDate=$3
endDate=$5
echo "startDate is $startDate"
echo "endDate is $endDate"
startDate=`date -d "-1 days "$startDate +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`
echo "startDate of data is $startDate"
echo "endDate of data is $endDate"

Params=($@)
MainClass=${Params[0]}
Length=${#Params[@]}
Args=${Params[@]:5:Length-5}

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh

load_properties ../conf/spark.properties

#params: $1 className, $2 propName
getSparkProp(){
    className=$1
    propName=$2

    defaultPropKey=${propName}
    defaultPropKey=${defaultPropKey//./_}
    defaultPropKey=${defaultPropKey//-/_}
    #echo "defaultPropValue=\$${defaultPropKey}"
    eval "defaultPropValue=\$${defaultPropKey}"

    propKey="${className}_${propName}"
    propKey=${propKey//./_}
    propKey=${propKey//-/_}
    eval "propValue=\$${propKey}"

    if [ -z "$propValue" ]; then
        echo "$defaultPropValue"
    else
        echo "$propValue"
    fi
}


spark_home=${spark_home:-$SPARK_HOME}
spark_master=${spark_master}
spark_mainJar="../lib/${spark_mainJarName}"
spark_driver_memory=$(getSparkProp $MainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $MainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $MainClass "spark.cores.max")
spark_shuffle_service_enabled=$(getSparkProp $MainClass "spark.shuffle.service.enabled")
spark_dynamicAllocation_enabled=$(getSparkProp $MainClass "spark.dynamicAllocation.enabled")
spark_dynamicAllocation_minExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.minExecutors")
spark_dynamicAllocation_maxExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.maxExecutors")
spark_dynamicAllocation_initialExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.initialExecutors")
spark_default_parallelism=$(getSparkProp $MainClass "spark.default.parallelism")
spark_yarn_queue=$(getSparkProp $MainClass "spark.yarn.queue")



for file in ../conf/*
do
	if [ -n "$resFiles" ]; then
		resFiles="$resFiles,$file"
	else
		resFiles="$file"
    fi
done

for file in /data/apps/azkaban/etl2/lib/*.jar
do
    if [[ "$file" == *${spark_mainJarName} ]]; then
        echo "skip $file"
    else
        if [ -n "$jarFiles" ]; then
            jarFiles="$jarFiles,$file"
        else
            jarFiles="$file"
        fi
    fi
done

while [[ $startDate -le $endDate ]]
do
    echo $startDate
    ts=`date +%Y%m%d_%H%M%S`
    set -x
    $spark_home/bin/spark-submit -v \
    --name ${app_name:-$MainClass}_$ts \
    --master ${spark_master} \
    --executor-memory $spark_executor_memory \
    --driver-memory $spark_driver_memory \
    --files $resFiles \
    --jars $jarFiles \
    --conf spark.cores.max=${spark_cores_max}  \
    --conf spark.shuffle.service.enabled=${spark_shuffle_service_enabled} \
    --conf spark.dynamicAllocation.enabled=${spark_dynamicAllocation_enabled}  \
    --conf spark.dynamicAllocation.minExecutors=${spark_dynamicAllocation_minExecutors} \
    --conf spark.dynamicAllocation.maxExecutors=${spark_dynamicAllocation_maxExecutors} \
    --conf spark.dynamicAllocation.initialExecutors=${spark_dynamicAllocation_initialExecutors} \
    --conf spark.default.parallelism=${spark_default_parallelism} \
    --conf spark.yarn.queue=${spark_yarn_queue} \
    --conf spark.sql.caseSensitive=true \
    --class "$MainClass" $spark_mainJar --startDate $startDate $Args
    if [ $? -ne 0 ];then
        echo "Execution failed, startDate of data is {$startDate}  ..."
        exit 1
    fi
    startDate=`date -d "1 days "$startDate +%Y%m%d`
done