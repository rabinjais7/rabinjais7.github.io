---
title: How Data Lakehouses Were Built: Spark, Hive and the Databricks Approach
date: 2020-12-05
author: Rabindra Jaiswal
categories: [Data Engineering, Apache Spark, Data Architecture]
---

## How Data Lakehouses Were Built: Spark, Hive and the Databricks Approach

Most companies started moving their data to the cloud because they wanted cheap storage and the flexibility to keep all kinds of data — structured, semi-structured, and unstructured. At that time the idea of a **data lakehouse** started becoming popular. This post explains in simple terms how people were building these systems and what Databricks did differently.

### The Traditional Way (Data Lake with Spark + Hive)

Before the lakehouse term became common, companies were building data lakes in this way:

- **Storage**: Cloud object storage (AWS S3, Azure Data Lake, or Google Cloud Storage) or HDFS. Data was simply dumped as files — mostly Parquet, ORC, or JSON.
- **Metadata**: Apache Hive Metastore was used to store table information (schema, partitions, and file locations). Hive allowed people to run SQL-like queries on those files.
- **Processing**: Apache Spark was the main processing engine. Data engineers wrote Spark jobs (mostly in Scala or Python) to read raw data, clean it, and write it back as Parquet files. Hive or Presto was also used for querying.
- **Layers**: People manually created folders such as `raw/`, `processed/`, and `curated/` and moved data between them using scheduled jobs.

**Common problems people faced:**

- No proper ACID transactions. If a job failed in the middle, data could become corrupted or only partially written.
- Updates and deletes were very difficult. You usually had to rewrite entire partitions or tables.
- Schema changes were risky and often caused failures.
- The small files problem was very common.
- Concurrent reads and writes were not safe.
- Data quality was hard to maintain. Many data lakes slowly turned into “data swamps.”

In short, storage was cheap and flexible, but reliability and manageability were poor compared to traditional data warehouses.

### What Databricks Did Differently

Databricks (the company founded by the creators of Spark) introduced the **Lakehouse** concept. The main idea was simple: take the cheap and open storage of a data lake and add the reliability and performance features of a data warehouse on top of it.

The key technology was **Delta Lake**.

**Delta Lake in simple words:**

- It stores data in Parquet format (so it remains open).
- On top of the Parquet files, it maintains a transaction log.
- This transaction log gives ACID transactions, schema enforcement, time travel, and easy support for `UPDATE`, `DELETE`, and `MERGE`.

Because of Delta Lake, Databricks could offer:

- Safe concurrent reads and writes
- Easy updates and upserts (very useful for slowly changing dimensions and change data capture)
- Schema enforcement and controlled schema evolution
- Time travel (you can query older versions of a table)
- Better performance through data skipping and file compaction (`OPTIMIZE`)
- Unified batch and streaming pipelines (same code path for both)

### Typical Architecture (Databricks Style)

Databricks popularised the **Medallion Architecture** (also called multi-hop architecture):

- **Bronze layer**: Raw data as it arrives. Minimal processing. Usually stored as Delta tables. Useful for audit and reprocessing.
- **Silver layer**: Cleaned, validated, and conformed data. Duplicates are removed, bad records are handled, and proper data types are applied.
- **Gold layer**: Business-level aggregates and models ready for reporting, dashboards, and machine learning.

Spark Structured Streaming together with Delta Lake made it possible to process both batch and streaming data in the same pipeline. Teams no longer needed to maintain two completely separate systems (the old Lambda architecture).

On the Databricks platform, people also got:

- Notebooks for development
- Jobs for scheduling and orchestration
- An optimised Spark runtime
- SQL Analytics capabilities for BI users

### Open Source vs Databricks Approach

In the open-source world, people were also trying to solve the same problems:

- **Apache Hudi** (used heavily by Uber) — strong for incremental processing and upserts.
- **Apache Iceberg** (originated at Netflix) — strong for large-scale table management and schema evolution.

Databricks made Delta Lake very easy to use on their platform and actively promoted the lakehouse idea. Many companies adopted Delta Lake because it worked well with Spark and solved the reliability problems they were facing with plain Parquet + Hive.

### Summary

| Aspect              | Traditional Data Lake                  | Databricks Lakehouse Approach              |
|---------------------|----------------------------------------|--------------------------------------------|
| Storage             | S3 / ADLS + Parquet / ORC              | S3 / ADLS + Delta Lake                     |
| Metadata            | Hive Metastore                         | Hive Metastore + Delta transaction log     |
| Transactions        | No proper ACID                         | Full ACID                                  |
| Updates / Deletes   | Difficult                              | Easy (`MERGE`, `UPDATE`, `DELETE`)         |
| Batch + Streaming   | Often separate systems                 | Unified                                    |
| Data Quality        | Hard to enforce                        | Schema enforcement + easier validation     |
| Popular Pattern     | Raw → Processed → Curated folders      | Bronze → Silver → Gold                     |

The shift from plain data lakes to lakehouses was mainly driven by the need for reliability. Teams were tired of dealing with corrupted data, failed jobs, and the difficulty of updating records. Databricks’ approach with Delta Lake gave a practical solution that many teams adopted.

If you were building a data platform, the practical choice was often between sticking with pure open-source Spark + Hive + Parquet (and handling reliability yourself) or using Databricks + Delta Lake to get reliability and easier operations out of the box.

