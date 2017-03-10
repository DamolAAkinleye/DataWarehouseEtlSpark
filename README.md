
test evn:
run machine:                 spark01
run lib:                     DataWarehouseEtlSpark-1.0.0-SNAPSHOT.jar
run script template:         /ftp/lituo/DataWarehouseEtlSpark/bin/startup.sh


jar包copy
scp -rC /Users/baozhiwang/Documents/nut/cloud/codes/DataWarehouseEtlSpark/target/DataWarehouseEtlSpark-1.0.0-SNAPSHOT-release/lib/DataWarehouseEtlSpark-1.0.0-SNAPSHOT.jar \
spark@bigdata-computing-02-018:/ftp/lituo/DataWarehouseEtlSpark/lib/


scp -rC /Users/baozhiwang/Documents/nut/cloud/codes/sparkThriftOnMesos/doc/scripts/init_hive_partition.sh spark@bigdata-computing-02-018:/ftp/michael/