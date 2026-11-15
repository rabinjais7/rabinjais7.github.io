---
title: Why Data Lakes Become Swamps and How the Lakehouse Paradigm Fixes Them
date: 2020-06-25
author: Rabindra Jaiswal
categories:
  - Data Engineering
  - Data Architecture
  - Apache Spark
  - Delta Lake
---

Here’s a cleaner, simpler rewrite in plain Indian English. I kept the Indian payments context, trimmed some long explanations, and made the tone sound more like you wrote it yourself.

---

title: Why Data Lakes Become Swamps and How the Lakehouse Fixes Them  
date: 2020-06-25  
author: Rabindra Jaiswal  
categories:  
  - Data Engineering  
  - Data Architecture  
  - Apache Spark  
  - Delta Lake  

# Why Data Lakes Become Swamps and How the Lakehouse Fixes Them

During Diwali, our payments platform sees huge spikes. Tens of millions of people scan BharatQR and UPI codes at kirana stores, pay bills, and send money to relatives. In the backend, microservices keep generating clickstream logs, API telemetry, payment status events, and database CDC changes all day and night.

We usually dump all this raw data straight into an object store like S3 or GCS. Storage is cheap and almost unlimited. It happily takes raw JSON, audio files from customer care bots, and CSV extracts. This is our classic data lake.

```
+-------------------------------------------------------------------------------+
|                           TRADITIONAL TWO-TIER SETUP                          |
|                                                                               |
|  [ UPI / Card Trans ] --+                                                     |
|  [ App Clickstream  ] --+--> [ Object Store (S3/GCS Data Lake) ]              |
|  [ Support Audio    ] --+          |                  |                       |
|                                    v                  v                       |
|                            [ Ad-hoc Spark / ML ]   [ Daily ETL Pipeline ]     |
|                               (Direct on Lake)        |                       |
|                                                       v                       |
|                                            [ Cloud Data Warehouse ]           |
|                                                       v                       |
|                                            [ BI Dashboards / SQL ]            |
+-------------------------------------------------------------------------------+
```

Then real work starts. The BI team needs to calculate merchant settlements as per RBI rules. When they run SQL on the raw lake using Presto or Spark, queries become very slow or fail.

So we build nightly pipelines to filter, aggregate, and copy a clean subset into a data warehouse. Now we maintain two systems: a cheap flexible lake for data science work, and an expensive rigid warehouse for daily MIS reports. Keeping both in sync is painful.

---

## Where the Traditional Data Lake Starts BreakingBut the problem starts when:
Multiple teams and jobs start reading and writing the same data at the same time (settlements, reports, streaming ingestion, corrections). Without isolation, people see half-written data, orphaned files, and race conditions between streaming and batch jobs.
We need to update or delete specific records later (DPDP requests, chargebacks, schema changes, data corrections). A raw lake only supports append, so every small change becomes a heavy rewrite of large files.
The volume of files and partitions grows. Millions of small files, slow metadata listing, and no proper statistics mean even simple queries become very slow, and we keep creating manual backup copies for audit requirements.
These three issues cover most of the day-to-day pain we face with a plain object store + Parquet setup. The rest of the problems (schema mismatches, metastore bottlenecks, Time Travel needs, etc.) are usually just symptoms of the same three root causes.

## How lakehouse solves this

Running a lake and a warehouse side by side means we pay twice for storage and keep writing glue jobs to keep them in sync. The Lakehouse idea is to have one system that gives us both flexibility and reliability.

```
+-------------------------------------------------------------------------------+
|                            THE LAKEHOUSE PARADIGM                             |
|                                                                               |
|  [ UPI / Card Trans ] --+                                                     |
|  [ App Clickstream  ] --+--> [ Object Storage: S3 / GCS ]                     |
|  [ Support Audio    ] --+          |                                          |
|                                    v                                          |
|                         [ Structured Transactional Layer ]                    |
|                         (Delta Lake: ACID, Schema, Metadata)                  |
|                                    |                                          |
|          +-------------------------+-------------------------+                |
|          v                                                   v                |
|  [ Ad-hoc Data Science & ML ]                     [ Direct BI & Analytics ]   |
|     (Python, R, PySpark)                              (SQL, Tableau, BI)      |
+-------------------------------------------------------------------------------+
```

We still store data as Parquet files in cheap object storage. On top of that we put a transactional layer (Delta Lake). This layer handles ACID transactions, schema checks, metadata, and better file layout. Data scientists and BI teams can both use the same data without copying it.

---

## How Delta Lake Tackles These Failures

Delta Lake does not replace Parquet. It adds a transaction log on top of it.

```
table_root/
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── 00000000000000000010.checkpoint.parquet
├── date=2020-06-25/
│   ├── part-00000-....parquet (active)
│   └── part-00001-....parquet (tombstoned)
```

Every change is recorded as a JSON commit in `_delta_log`. Readers only look at files listed in the log. Uncommitted files are invisible. Optimistic concurrency handles simultaneous writes. If two jobs touch different files, both succeed. If they conflict, one retries.

Because the log controls the table, updates and deletes become possible:

```sql
DELETE FROM payment_lake.customers
WHERE user_phone_number = '+919876543210';
```

Delta rewrites only the affected files and marks the old ones as removed. Old files stay for Time Travel, so we can query the exact state of the ledger on 31st March 2020 without keeping separate backup copies.

For large tables, Delta writes Parquet checkpoint files every 10 commits. Query engines use Spark to read the checkpoint + latest JSON logs. Metadata scales with the data instead of depending on a single metastore database.

To fix small files we run:

```sql
OPTIMIZE payment_lake.settlements
WHERE date >= '2020-06-01';
```

This merges small files into larger ones without blocking readers. Delta also stores min/max statistics, so queries can skip files that cannot contain matching rows. For multi-column filters we use Z-Ordering:

```sql
OPTIMIZE payment_lake.settlements
ZORDER BY (payer_vpa, merchant_category_code);
```

This keeps related values together so the engine can prune better on both columns.

Schema enforcement is strict by default. Wrong schema fails the write. If we want evolution we pass `mergeSchema`. We can also add business rules:

```sql
ALTER TABLE payment_lake.settlements 
ADD CONSTRAINT valid_amount CHECK (amount > 0 AND amount <= 1000000);
```

Bad records are rejected before they enter the table.

---

## The Medallion Architecture

Once we have a reliable lake, we organise data in three layers:

```
+---------------------------------------------------------------------------------------+
|                                MEDALLION ARCHITECTURE                                 |
|                                                                                       |
|   +-------------------+       +--------------------+       +----------------------+   |
|   |   BRONZE LAYER    |  -->  |    SILVER LAYER    |  -->  |      GOLD LAYER      |   |
|   |    (Raw Append)   |       | (Cleaned & Conformed)      |  (Business Aggregates)   |
|   +-------------------+       +--------------------+       +----------------------+   |
|   • Raw JSON / Avro           • Cleaned data types         • Dimension & Fact tables  |
|   • Append-only logs          • Validated constraints      • Pre-aggregated KPIs      |
|   • Preserves full history    • Enriched & deduped         • Optimized for BI / MIS   |
+---------------------------------------------------------------------------------------+
```

- **Bronze**: Raw append-only data. Streaming events and CDC land here as-is. We keep full history so we can reprocess if needed.
- **Silver**: Cleaned and typed data. We parse JSON, fix time zones to IST, deduplicate, and apply quality checks. This is the main table for data science and ad-hoc analysis.
- **Gold**: Business-ready aggregates and star schemas for dashboards and MIS. Queries are fast because the data is already clean and optimised.

---

## Practical Implementation

### Bronze (raw streaming)

```python
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker.internal:9092") \
    .option("subscribe", "raw-payments") \
    .load()

bronze_stream = raw_kafka_df.select(
    col("value").cast("string").alias("payload"),
    current_timestamp().alias("ingestion_time")
).writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/lakehouse/checkpoints/payments_bronze") \
    .start("/lakehouse/bronze/payments")
```

### Silver (parse + merge)

```python
def upsert_to_silver(micro_batch_df, batch_id):
    parsed_batch = micro_batch_df.select(
        from_json(col("payload"), payment_schema).alias("data")
    ).select("data.*").filter("amount > 0")

    if not DeltaTable.isDeltaTable(spark, silver_table_path):
        parsed_batch.write.format("delta").save(silver_table_path)
    else:
        silver_table = DeltaTable.forPath(spark, silver_table_path)
        silver_table.alias("tgt").merge(
            parsed_batch.alias("src"),
            "tgt.txn_id = src.txn_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
```

### Gold (aggregates)

```sql
CREATE OR REPLACE TABLE payment_lake.gold_daily_merchant_settlements
USING DELTA
LOCATION '/lakehouse/gold/daily_merchant_settlements'
AS
SELECT 
    CAST(txn_time AS DATE) AS settlement_date,
    merchant_id,
    COUNT(txn_id) AS total_successful_transactions,
    SUM(amount) AS total_settled_amount
FROM delta.`/lakehouse/silver/payments`
WHERE status = 'SUCCESS'
GROUP BY CAST(txn_time AS DATE), merchant_id;

OPTIMIZE payment_lake.gold_daily_merchant_settlements
ZORDER BY (merchant_id);
```

---

## Trade-offs to Keep in Mind

1. Delta is good for analytical micro-batches, not for thousands of tiny point updates per second. High-frequency `MERGE`s can increase latency and cause retries.
2. Old files stay for Time Travel, so storage grows. We must run `VACUUM` carefully (usually retain 7 days). If we vacuum too aggressively, long-running queries can fail.
3. Different engines (Spark, Trino, Athena, Snowflake) support Delta at different levels. Check the protocol version your writes use before relying on advanced features.

---

## Conclusion

The old choice between data lake and data warehouse was never just about storage. We needed cheap open storage for large raw data *and* reliable ACID behaviour with good performance for SQL analytics.

The Lakehouse with Delta Lake gives us both in one place. Engineers get transactions and schema checks. Analysts get fast queries. Data scientists still get direct access to the raw data. We stop maintaining two separate systems and stop fighting reconciliation issues.

With this approach, the data lake stops behaving like a swamp and starts working like a proper warehouse.