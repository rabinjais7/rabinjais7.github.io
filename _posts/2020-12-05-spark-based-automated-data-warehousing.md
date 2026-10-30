---
title: Building a Metadata Driven Spark Framework with Data Vault and Scala
date: 2020-12-05
author: Rabindra Jaiswal
categories: [Data Engineering, Apache Spark, Data Architecture]
---

# Building a Metadata Driven Spark Framework with Data Vault and Scala

Every growing engineering team in India eventually hits the pipeline wall. Imagine you are working at a fast-growing retail chain or a digital payments platform like PhonePe or Paytm. At first, there are only three upstream systems: an internal PostgreSQL database recording checkout transactions, a few flat files dumped on an SFTP server by third-party logistics partners, and an ancient Oracle database sitting inside a regional warehouse in Bhiwandi.

To get data into your warehouse, you write a Spark job for each table. It feels productive. You write a script for `customers`, one for `orders`, and another for `store_inventory`. 

Within a year, the business expands to tier-2 and tier-3 towns. You now have fifty microservices emitting events into Apache Kafka, vendor bills landing as Parquet on MinIO or AWS S3, and marketing teams demanding real-time feeds into Delta Lake and legacy reporting databases. Suddenly, your repository has 250 individual Spark jobs. 

When a schema changes upstream—say a payment gateway adds a new field for UPI reference numbers—three engineers spend their entire weekend debugging broken DAGs and redeploying jars. Most of your time is not spent solving data engineering problems; it is spent doing routine plumbing.

The only way out of this trap is to stop writing individual pipelines and start writing a metadata-driven processing engine. By combining the structural predictability of Data Vault 2.0 with the clean decoupling of the Abstract Factory pattern in Scala, you can reduce hundreds of fragile jobs into three generic, robust pipeline patterns.

---

## The Trap of Traditional Dimensional Modeling

In traditional Kimball-style dimensional modeling, every pipeline has to know too much business logic upfront. You have to handle slowly changing dimensions (SCD Type 2), resolve surrogate foreign keys, join five tables to confirm if a record exists, and merge facts. Because every source table has a different shape and business meaning, you end up writing custom extraction, transformation, and load scripts for every entity.

If an enterprise has 40 sources and writes to 3 different targets (say Delta Lake for ad-hoc queries, an Oracle database for operational finance reports, and raw HDFS for archiving), the combinations explode. You are looking at maintaining $N \times M$ code paths.

```
[40 Sources: Kafka, Oracle, Postgres, CSV]
                     │
         (Dozens of custom Spark jobs)
                     ▼
[3 Targets: Delta Lake, Oracle DB, S3 / HDFS]
```

When you look closely at what these pipelines actually do, ninety percent of the logic is repeated boilerplate:
1. Connect to source $A$.
2. Read the delta records since the last checkpoint.
3. Compute a hash key to identify the record.
4. Check if the record already exists in target $B$.
5. If changed, insert a new record or update the existing one.
6. Commit the offsets and write audit logs.

If the underlying table structure can be standardized, the transformation logic becomes identical for every single pipeline. This is where Data Vault 2.0 comes into the picture.

---

## Why Data Vault 2.0 Simplifies the Engine

Data Vault 2.0 splits data into three distinct structural components based on business concepts rather than source system schemas:

1. **Hubs**: Store unique business keys. A hub contains only the immutable identity of an entity (like an Aadhaar number, PAN card number, GSTIN, or Customer ID), a surrogate hash key, a load timestamp, and the record source. Hubs never change; they only append.
2. **Links**: Represent relationships or transactions between hubs. A link connecting a customer hub and a store hub represents a purchase. Like hubs, links contain hash keys, foreign hash keys pointing to the connected hubs, load timestamps, and source details.
3. **Satellites**: Hold the contextual, descriptive attributes that change over time (such as customer name, mobile number, delivery address, or transaction status). Satellites provide full historization by storing the record with a timestamp and a hash difference (`hash_diff`) column.

```
       ┌─────────────────┐               ┌─────────────────┐
       │   Hub Customer  │               │    Hub Store    │
       │─────────────────│               │─────────────────│
       │ HK_Customer (PK)│               │  HK_Store (PK)  │
       │ Customer_ID (BK)│               │  Store_Code(BK) │
       │ Load_Date       │               │  Load_Date      │
       │ Record_Source   │               │  Record_Source  │
       └────────┬────────┘               └────────┬────────┘
                │                                 │
                │        ┌──────────────┐         │
                └───────►│  Link_Sales  │◄────────┘
                         │──────────────│
                         │ HK_Link (PK) │
                         │ HK_Customer  │
                         │ HK_Store     │
                         │ Load_Date    │
                         │ Record_Source│
                         └──────┬───────┘
                                │
                         ┌──────▼──────────────┐
                         │   Sat_Sales_Detail  │
                         │─────────────────────│
                         │ HK_Link (FK)        │
                         │ Load_Date (PK)      │
                         │ Hash_Diff           │
                         │ Amount, Tax, Mode   │
                         │ Record_Source       │
                         └─────────────────────┘
```

Because Data Vault guarantees that every entity in your entire business architecture maps to either a Hub, a Link, or a Satellite, you no longer need hundreds of custom pipelines. **You only need three pipeline templates in your entire framework:**

- A template to load Hubs.
- A template to load Links.
- A template to load Satellites.

Whether you are loading customer records from an online Kirana grocery app or loan repayment data from an NBFC core banking system, the transformation mathematics inside Spark remain completely identical.

---

## Implementing the Three Processing Patterns in Spark

To build this in Apache Spark using Scala, we implement pure transformation functions for each Data Vault primitive. We rely on cryptographic hashing (typically MD5 or SHA-256) rather than auto-incrementing database sequences. This allows distributed nodes to compute surrogate keys in parallel without coordinating with a central database.

### 1. The Hub Loader

The Hub loader accepts incoming data, extracts the business key, removes null or malformed entries, calculates the hash key, and keeps only the distinct keys before appending them to the target Hub table.

```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object HubTransformer {

  def transformHub(
      sourceDf: DataFrame,
      businessKeyCol: String,
      targetHubKey: String,
      recordSource: String
  ): DataFrame = {
    sourceDf
      .filter(col(businessKeyCol).isNotNull && trim(col(businessKeyCol)) =!= "")
      .withColumn("clean_bk", upper(trim(col(businessKeyCol).cast(StringType))))
      .withColumn(targetHubKey, sha2(col("clean_bk"), 256))
      .withColumn("LOAD_DATETIME", current_timestamp())
      .withColumn("RECORD_SOURCE", lit(recordSource))
      .select(
        col(targetHubKey),
        col("clean_bk").as(businessKeyCol),
        col("LOAD_DATETIME"),
        col("RECORD_SOURCE")
      )
      .dropDuplicates(Seq(targetHubKey))
  }
}
```

When writing this DataFrame to the target table, the producer simply performs an anti-join against the existing Hub to filter out keys that have already been recorded:

```scala
val newHubRecords = transformedHubDf.join(
  existingHubDf,
  transformedHubDf(targetHubKey) === existingHubDf(targetHubKey),
  "left_anti"
)
```

### 2. The Link Loader

A Link represents an interaction between two or more Hubs. For instance, in an Indian logistics setup, a delivery record ties a `consignment_id` to a `delivery_partner_id` and a `hub_location_id`.

```scala
object LinkTransformer {

  def transformLink(
      sourceDf: DataFrame,
      linkKeyName: String,
      hubKeyMappings: Map[String, String], // Map("BK_COLUMN" -> "HUB_HASH_KEY_NAME")
      recordSource: String
  ): DataFrame = {
    var intermediateDf = sourceDf

    // Clean and generate hash keys for each referenced hub
    val hubHashCols = hubKeyMappings.map { case (bkCol, hubHashCol) =>
      val cleanColName = s"clean_${bkCol}"
      intermediateDf = intermediateDf
        .filter(col(bkCol).isNotNull)
        .withColumn(cleanColName, upper(trim(col(bkCol).cast(StringType))))
        .withColumn(hubHashCol, sha2(col(cleanColName), 256))
      col(hubHashCol)
    }.toSeq

    // Link hash key is a hash of all concatenated hub hash keys in deterministic order
    val concatHubKeys = concat_ws("||", hubHashCols: _*)
    
    intermediateDf
      .withColumn(linkKeyName, sha2(concatHubKeys, 256))
      .withColumn("LOAD_DATETIME", current_timestamp())
      .withColumn("RECORD_SOURCE", lit(recordSource))
      .select((Seq(col(linkKeyName)) ++ hubHashCols ++ Seq(col("LOAD_DATETIME"), col("RECORD_SOURCE"))): _*)
      .dropDuplicates(Seq(linkKeyName))
  }
}
```

### 3. The Satellite Loader

Satellites capture descriptive context and handle historization. Instead of updating rows directly in a table (like an expensive SQL `MERGE` statement across large partitions), the Satellite pattern relies on an append-only architecture. 

We generate a `hash_diff` column across all descriptive attributes. If any attribute changes, the `hash_diff` changes, and a new record is appended with a new `LOAD_DATETIME`.

```scala
object SatelliteTransformer {

  def transformSatellite(
      sourceDf: DataFrame,
      parentHashKeyCol: String,
      descriptiveCols: Seq[String],
      recordSource: String
  ): DataFrame = {
    // Sort column names alphabetically to ensure deterministic hash generation across runs
    val sortedCols = descriptiveCols.sorted.map { c =>
      coalesce(trim(col(c).cast(StringType)), lit("^^NULL^^"))
    }

    val hashDiffExpression = sha2(concat_ws("||", sortedCols: _*), 256)

    sourceDf
      .filter(col(parentHashKeyCol).isNotNull)
      .withColumn("HASH_DIFF", hashDiffExpression)
      .withColumn("LOAD_DATETIME", current_timestamp())
      .withColumn("RECORD_SOURCE", lit(recordSource))
      .select(
        (Seq(col(parentHashKeyCol), col("HASH_DIFF"), col("LOAD_DATETIME"), col("RECORD_SOURCE")) ++ 
         descriptiveCols.map(col)): _*
      )
  }
}
```

The beauty of this design is immediately obvious: whether you are processing customer profiles, vendor bank accounts, or IoT telemetry from delivery trucks, you never rewrite transformation code. You just execute these functions with different metadata parameters.

---

## Decoupling I/O with the Abstract Factory Pattern

Having unified transformation logic solves only half the problem. What happens when source data comes from Kafka topics during peak business hours, but arrives as batch Parquet files via HDFS during the night? What if one team writes to a Delta Lake table while another requires the curated output pushed to an external Oracle instance for regulatory compliance?

If you embed `spark.read.format("kafka")` or `spark.read.parquet()` directly inside your transformation jobs, the codebase turns into spaghetti. 

We solve this using the **Abstract Factory Pattern**. We enforce a clean separation of concerns:
- **Consumer**: Reads data from any source and exposes a raw DataFrame.
- **Engine Core**: Executes the generic Data Vault transformations.
- **Producer**: Writes the resulting DataFrame into any storage format.

```
       ┌────────────────────────┐
       │   ConsumerFactory      │
       └───────────┬────────────┘
                   │ creates
     ┌─────────────┴─────────────┐
     ▼                           ▼
┌──────────────┐          ┌──────────────┐
│KafkaConsumer │          │ HdfsConsumer │
└──────┬───────┘          └──────┬───────┘
       │                         │
       └───────────► DataFrame ◄─┘
                         │
                         ▼
             [ Data Vault Transformers ]
             (Hub, Link, or Satellite)
                         │
                         ▼
                   DataFrame
                         │
       ┌─────────────────┴─────────────┐
       ▼                               ▼
┌──────────────┐                ┌──────────────┐
│DeltaProducer │                │OracleProducer│
└──────────────┘                └──────────────┘
     ▲                                 ▲
     │ creates                         │
     └─────────────┬───────────────────┘
       ┌───────────┴────────────┐
       │    ProducerFactory     │
       └────────────────────────┘
```

### Implementing Consumer Interfaces

First, define a clean trait for the Consumer:

```scala
package com.dataplatform.framework.io

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SourceConsumer {
  def read(spark: SparkSession, options: Map[String, String]): DataFrame
}

class KafkaSourceConsumer extends SourceConsumer {
  override def read(spark: SparkSession, options: Map[String, String]): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", options("bootstrap_servers"))
      .option("subscribe", options("topic"))
      .option("startingOffsets", options.getOrElse("starting_offsets", "latest"))
      .load()
      .selectExpr("CAST(value AS STRING) as json_payload")
  }
}

class HdfsParquetConsumer extends SourceConsumer {
  override def read(spark: SparkSession, options: Map[String, String]): DataFrame = {
    spark.read
      .parquet(options("path"))
  }
}
```

### Implementing Producer Interfaces

Next, define the contract for writing out the transformed DataFrames:

```scala
trait TargetProducer {
  def write(df: DataFrame, options: Map[String, String]): Unit
}

class DeltaLakeProducer extends TargetProducer {
  override def write(df: DataFrame, options: Map[String, String]): Unit = {
    val writer = df.write
      .format("delta")
      .mode(options.getOrElse("save_mode", "append"))

    options.get("partition_by").foreach { partitionCols =>
      writer.partitionBy(partitionCols.split(","): _*)
    }

    writer.save(options("target_path"))
  }
}

class JdbcProducer extends TargetProducer {
  override def write(df: DataFrame, options: Map[String, String]): Unit = {
    df.write
      .format("jdbc")
      .option("url", options("jdbc_url"))
      .option("dbtable", options("dbtable"))
      .option("user", options("user"))
      .option("password", options("password"))
      .option("driver", options("driver"))
      .mode(options.getOrElse("save_mode", "append"))
      .save()
  }
}
```

### The Abstract Factories

Now, create lightweight factories to instantiate these drivers based on simple string parameters:

```scala
object ConsumerFactory {
  def getConsumer(sourceType: String): SourceConsumer = {
    sourceType.toLowerCase match {
      case "kafka" => new KafkaSourceConsumer()
      case "hdfs" | "parquet" => new HdfsParquetConsumer()
      case unknown => throw new IllegalArgumentException(s"Unsupported source type: $unknown")
    }
  }
}

object ProducerFactory {
  def getProducer(targetType: String): TargetProducer = {
    targetType.toLowerCase match {
      case "delta" => new DeltaLakeProducer()
      case "jdbc" | "oracle" | "postgres" => new JdbcProducer()
      case unknown => throw new IllegalArgumentException(s"Unsupported target type: $unknown")
    }
  }
}
```

With this architecture in place, our pipeline driver does not contain any hardcoded storage references. The developer simply glues them together:

```scala
val consumer = ConsumerFactory.getConsumer(config.sourceType)
val rawData = consumer.read(spark, config.sourceOptions)

val transformedData = HubTransformer.transformHub(
  rawData,
  config.businessKeyColumn,
  config.targetKeyName,
  config.recordSource
)

val producer = ProducerFactory.getProducer(config.targetType)
producer.write(transformedData, config.targetOptions)
```

Adding support for an entirely new storage layer—such as Snowflake, Google Cloud Storage, or an Apache Iceberg catalog—requires writing a single class that satisfies the `TargetProducer` trait and registering it in `ProducerFactory`. Not a single line of your Data Vault transformation logic changes.

---

## Driving Everything Through Metadata

Because both the transformation models and the I/O layers are standardized, the execution of any pipeline can be defined entirely as a configuration file.

Whether you store these definitions in an internal PostgreSQL configuration table, a centralized schema registry, or simple YAML/JSON files stored in Git, your runtime engine simply parses the config and executes the run.

Here is an example configuration for extracting user registration events from a Kafka topic and publishing them to a Delta Lake Hub:

```json
{
  "pipeline_name": "load_hub_customer_from_kafka",
  "pipeline_type": "HUB",
  "source": {
    "type": "kafka",
    "options": {
      "bootstrap_servers": "kafka-broker.internal:9092",
      "topic": "identity.users.registered",
      "starting_offsets": "earliest"
    }
  },
  "transformation": {
    "business_key": "mobile_number",
    "target_hash_key": "HK_CUSTOMER",
    "record_source": "SRV_IDENTITY_KAFKA"
  },
  "target": {
    "type": "delta",
    "options": {
      "target_path": "s3a://lakehouse-gold/vault/hub_customer/",
      "save_mode": "append"
    }
  }
}
```

When a new business team arrives asking for their data to be ingested into your enterprise lakehouse, you no longer schedule sprints to write code. You write a 25-line configuration file, validate it against your JSON schema, run automated integration tests, and schedule it on Apache Airflow.

---

## Operational Trade-Offs and Real-World Limitations

While this architecture brings structural order to data engineering teams, it is not a silver bullet. Designing data platforms for Indian-scale data volumes means confronting real operational constraints.

### 1. Storage Amplification and Small Files
Data Vault is inherently verbose. Breaking a single source table with 80 columns into one Hub, two Links, and three Satellites means you are now writing six distinct tables instead of one. 
- Every table maintains its own metadata, its own hash keys, timestamps, and audit columns.
- If you run micro-batch streaming jobs against these tables every 5 minutes, you will quickly generate millions of tiny files on object storage.
- **Mitigation:** You must run proactive file compaction (e.g., `OPTIMIZE` and `VACUUM` commands in Delta Lake, or automated compaction jobs in Iceberg) to prevent query degradation on your underlying storage.

### 2. Hash Collision Realities
Most Data Vault implementations use MD5 (128-bit) or SHA-256 (256-bit) to calculate surrogate hash keys. While MD5 is computationally cheaper, security audits in banking and fintech environments often mandate SHA-256. 
- Calculating SHA-256 strings across tens of millions of wide records inside Spark adds measurable CPU overhead. 
- **Mitigation:** Ensure that your string concatenation expressions use clean, consistent delimiters (like `||`) and explicit null replacements (like `^^NULL^^`) so that `NULL` followed by `'A'` does not hash to the same value as `'A'` followed by `NULL`.

### 3. Join Overhead in Downstream Analytics
Business analysts and reporting tools (like Power BI, Tableau, or Apache Superset) do not understand Hubs and Satellites. They expect flattened, denormalized dimensional tables or star schemas.
- Reconstructing a single entity view from Data Vault requires joining the Hub with its active Satellite records, which triggers Spark shuffles across huge tables.
- **Mitigation:** Never expose raw Data Vault tables directly to business users. Build an automated "Business Vault" or "Information Mart" layer on top of your raw vault. Use Point-in-Time (PIT) tables and Bridge tables to pre-calculate and materialize these joins on a scheduled basis.

```
[Raw Sources] ──► [Metadata-Driven Ingestion] ──► [Raw Data Vault: Hubs, Links, Sats]
                                                                │
                                                                ▼
                                                   [Automated PIT / Bridge Tables]
                                                                │
                                                                ▼
                                                [Star Schema Marts / Reporting Views]
```

### 4. Late-Arriving Data and Hash Drift
If source systems send updates out of order (a common scenario when dealing with offline POS systems in remote areas), Satellites can record incorrect chronological timelines if you blindly rely on the arrival timestamp (`LOAD_DATETIME`).
- **Mitigation:** Maintain two distinct timestamps in your Satellites: the `APPLIED_DATE` (when the transaction actually happened at the source terminal) and the `LOAD_DATE` (when your Spark pipeline ingested the record). Historization logic must take both into account when building point-in-time snapshots.

---

## Final Thoughts

The habit of treating every new business requirement as an excuse to build a brand new ETL pipeline is the primary reason data platform teams get overwhelmed. Teams spend eighty percent of their capacity keeping existing jobs alive rather than building reusable capabilities.

By standardizing your storage layer around Data Vault primitives and strictly enforcing separation of concerns using classic software engineering patterns like Abstract Factory, you fundamentally change the productivity curve of your team. You stop writing repetitive Spark jobs and start maintaining a platform that processes data predictably, reliably, and at scale.