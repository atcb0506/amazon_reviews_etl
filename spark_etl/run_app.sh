#!/bin/bash

spark-submit \
--packages=org.apache.hadoop:hadoop-aws:2.7.3 \
--master=local[*] \
--name=local_spark_job \
main.py \
--mode=$RUN_MODE