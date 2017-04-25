#!/usr/bin/env bash

alias fget='python /data/tscripts/scripts/ftp.py -s get -f '
fget DataWarehouseEtlSpark-1.0.0-michael.jar
mv DataWarehouseEtlSpark-1.0.0-michael.jar ./../lib/DataWarehouseEtlSpark-1.0.0.jar
md5sum ./../lib/DataWarehouseEtlSpark-1.0.0.jar

one_day=$1
echo "Play start"
sh submit.sh cn.whaley.datawarehouse.fact.moretv.Play --startDate ${one_day} --deleteOld true
