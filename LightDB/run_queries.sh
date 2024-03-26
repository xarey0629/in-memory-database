#!/bin/bash

for i in {1..18}
do
    command="java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query${i}.sql samples/output/query${i}.csv"
    echo "Running command: $command"
    $command
done
