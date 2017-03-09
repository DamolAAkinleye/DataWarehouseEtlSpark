#!/usr/bin/env bash

main_class="cn.whaley.datawarehouse.temp.IncrementNewTest"
current_bin_path="`dirname "$0"`"
cd ${current_bin_path}
echo "current_bin_path is ${current_bin_path}"

echo "数据不上线,数据结果生成到(tmp/维度)目录里用来检查数据正确定"
sh submit.sh ${main_class} --isOnline false

echo "数据上线"
sh submit.sh ${main_class} --isOnline true
