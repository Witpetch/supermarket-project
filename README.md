# Building an End-to-End Data Pipeline for Supermarket sales Analysis on AWS Cloud Platform
---
Hi! this is my data engineering project. Full version on Medium : # [Supermarket sales Analysis ; Data pipeline end-to-end Project [Medium]](https://medium.com/@chawanwit.petch/building-an-end-to-end-data-pipeline-for-supermarket-sales-analysis-on-aws-cloud-platform-6e21f7c5085b) (SQLs are on medium)
# Tech Stack
- Amazon S3
- AWS Glue Data Catalog
- AWS Glue Crawler
- AWS Glue ETL Job
- Amazon Quicksight
- Apache Spark
- SQL
- Python

---

# Overview
In this project, we'll set up an efficient data processing pipeline. Starting with the creation of a new S3 bucket and uploading a remote CSV file, we'll establish a Data Catalog using a Crawler. Subsequently, a Glue ETL Job, driven by a Spark script, will handle data transformations. The refined data will be stored as a parquet file in S3, accompanied by a Data Catalog. Data querying will be conducted through Athena and S3 Select. The project culminates in the creation of a dashboard using Amazon QuickSight. As part of our production pipeline, we anticipate periodic modifications to the CSV file in S3, while maintaining a consistent schema. Our goal is to generate corresponding modified parquet files in a separate directory to uphold the integrity of the pipeline.

---

# ETL Method
![data flowchart]![data flowchart supremarket](https://github.com/Witpetch/supermarket-project/assets/126334722/02371c86-9bde-47a2-b294-e7173a485346)
