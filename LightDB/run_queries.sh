#!/bin/bash

# 循环遍历 1 到 12
for i in {1..12}
do
    # 构建命令
    command="java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query${i}.sql samples/output/query${i}.csv"
    
    # 执行命令
    echo "Running command: $command"
    $command
done
