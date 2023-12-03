# hadoop-spark-pipeline
An ETL pipeline that extracts data from HDFS , transforms using spark and writes back to HDFS.

## Hadoop Spark Pipeline

This repository contains a Scala script that implements a simple data pipeline using Hadoop and Spark.

## Purpose of the Pipeline

The purpose of this pipeline is to:

1. Read data from HDFS.
2. Perform JOIN operations using Spark.
3. Perform data analysis and transformations.
4. Write the processed data back to HDFS.

## How to Use

- Ensure you have Java, Hadoop and Spark installed.
- Execute the Scala script (`pipeline2.scala`) to run the data pipeline.
- Command to execute: `spark-shell -I "path/to/file.scala"`

