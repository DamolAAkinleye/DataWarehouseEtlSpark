
test evn:
run machine:                 spark18
run lib:                     DataWarehouseEtlSpark-1.0.0-SNAPSHOT.jar
run script template:         /ftp/lituo/DataWarehouseEtlSpark/bin/startup.sh






jar包copy
cp /Users/baozhiwang/Documents/nut/cloud/codes/DataWarehouseEtlSpark/target/DataWarehouseEtlSpark-1.0.0.jar /Users/baozhiwang/Documents/upload_dir/DataWarehouseEtlSpark-1.0.0-michael.jar
md5 /Users/baozhiwang/Documents/upload_dir/DataWarehouseEtlSpark-1.0.0-michael.jar

运行：
spark@bigdata-appsvr-130-5
hadoop fs -rm -r /data_warehouse/dw_facts/fact_medusa_play/20170411
cd /opt/dw/etl/bin;sh play.sh 20170425

hadoop fs -ls /data_warehouse/dw_facts/fact_medusa_play/20170411/00

