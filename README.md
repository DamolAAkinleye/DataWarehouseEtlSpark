
test evn:
run machine:     spark01
run lib:         DataWarehouseEtlSpark-1.0.0-SNAPSHOT.jar
run script:      /ftp/lituo/DataWarehouseEtlSpark-1.0.0-SNAPSHOT-release/bin/





scp -rC /Users/baozhiwang/Documents/nut/cloud/codes/DataWarehouseEtlSpark/target/DataWarehouseEtlSpark-1.0.0-SNAPSHOT-release/lib/DataWarehouseEtlSpark-1.0.0-SNAPSHOT.jar \
spark@bigdata-computing-01-001:/ftp/lituo/DataWarehouseEtlSpark-1.0.0-SNAPSHOT-release/lib



 