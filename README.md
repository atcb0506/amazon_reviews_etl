# Amazon review ETL

## 01. Introduction

This project is to analyse amazon reviews as provided by aws. [Link here]

[Link here]: https://registry.opendata.aws/amazon-reviews/#usageexamples


|Content|Directory|Description|
|---|---|---|
|ETL|[spark_etl]| ETL for extracting review metrics for recommendation system|
|DDL|[db]| DDL for this project|
|EMR deployer|[emr_deployer]| Deploy and run the [spark_etl] on AWS EMR|


[spark_etl]: ./spark_etl
[db]: ./db
[emr_deployer]: ./emr_deployer