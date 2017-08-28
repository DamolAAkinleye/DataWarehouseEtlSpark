
在服务器上测试
本地电脑打包后，jar包copy
cp /Users/baozhiwang/Documents/nut/cloud/codes/DataWarehouseEtlSpark/target/DataWarehouseEtlSpark-1.0.0.jar ~/Documents/upload_dir/
md5 ~/Documents/upload_dir/DataWarehouseEtlSpark-1.0.0.jar

运行：
new test on spark@bigdata-appsvr-130-5 
cd /opt/dw/etl2;
bin/submit.sh cn.whaley.datawarehouse.fact.moretv.PlayFinal --startDate 20170815 --isOnline false