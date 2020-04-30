#!/bin/bash

echo "Running Amazon Review ETL..."

if [ $ENV_EXECUTION_TYPE = local ]
then
  echo "Run in local mode"
  spark-submit \
  --packages=org.apache.hadoop:hadoop-aws:2.7.3 \
  --master=local[*] \
  --name=local_amazon_review_etl_spark_job \
  main.py \
  --mode=$ENV_RUN_MODE
else
  echo "Run in cluster mode"
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  export SPARK_HOME=/usr/lib/spark
  export HADOOP_CONF_DIR=/etc/hadoop/conf
  export PYTHONPATH=$SPARK_HOME/python
  export PYSPARK_PYTHON=/usr/bin/python3
  export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

  spark-submit \
  --master=yarn \
  --deploy-mode=cluster \
  --name=cluster_amazon_review_etl_spark_job \
  --py-files=spark_etl_package.zip \
  main.py \
  --mode=$ENV_RUN_MODE
fi