---
title: Data Quality in the Lakehouse Practical Ways to Keep Your Data Trustworthy
date: 2024-07-31
author: Rabindra Jaiswal
categories:
  - Data Engineering
  - Azure
  - Databricks
  - Delta Lake
  - Data Architecture
  - ETL

---

# Data Quality in the Lakehouse: Practical Ways to Keep Your Data Trustworthy

In a lakehouse, data comes from many sources and moves through multiple layers before it reaches reports, dashboards or machine learning models. If bad data enters the system, it can silently affect business decisions. That is why data quality is not optional — it must be built into the pipeline.

In this post we will see practical ways to improve data quality on Databricks using Delta Lake and Delta Live Tables. The language is kept simple so anyone working with Spark can follow it.

### Why Data Quality Matters in a Lakehouse

A lakehouse stores both raw and curated data in the same place. Because of this flexibility, it is easy for null values, duplicates, wrong data types or unexpected values to enter the system. Once bad data reaches the Gold layer, fixing it becomes expensive.

Good data quality practices help you:

- Catch problems early
- Stop bad records from moving forward
- Keep a record of what failed and why
- Make pipelines more reliable

### 1. Schema Enforcement with Delta Lake

Delta Lake automatically checks the schema when you write data. If the incoming data does not match the table schema, the write fails. This is one of the simplest and most useful protections.

```python
# This will fail if the DataFrame has a different schema
df.write.format("delta").mode("append").saveAsTable("silver.orders")
```

You can also control schema evolution:

```python
# Allow new columns to be added
df.write.format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .saveAsTable("silver.orders")
```

Use schema enforcement in Bronze and Silver layers so unexpected columns or type mismatches are caught early.

### 2. Table Constraints (NOT NULL and CHECK)

Delta Lake supports `NOT NULL` and `CHECK` constraints. These are checked at write time.

```sql
-- Create table with constraints
CREATE TABLE silver.customers (
  customer_id   BIGINT NOT NULL,
  email         STRING,
  age           INT,
  CONSTRAINT valid_age CHECK (age >= 0 AND age <= 120),
  CONSTRAINT valid_email CHECK (email LIKE '%@%.%')
) USING DELTA;

-- Add constraint on existing table
ALTER TABLE silver.orders 
ADD CONSTRAINT positive_amount CHECK (order_amount > 0);
```

If any row violates the constraint, the entire write fails. This is useful for critical columns such as primary keys, amounts and dates.

### 3. Data Quality with Delta Live Tables Expectations

Delta Live Tables (DLT) makes data quality easier with **Expectations**. An expectation is a simple SQL condition. You can decide what should happen when a record fails the condition:

- **Warn** (default) — keep the record and just record the failure
- **Drop** — remove the bad record
- **Fail** — stop the pipeline

**Python example:**

```python
import dlt

@dlt.table
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "order_amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('NEW', 'SHIPPED', 'DELIVERED')")
def silver_orders():
  return spark.readStream.table("bronze.orders")
```

**SQL example:**

```sql
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL),
  CONSTRAINT positive_amount EXPECT (order_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('NEW', 'SHIPPED', 'DELIVERED')) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM(bronze.orders);
```

DLT shows quality metrics in the pipeline UI and stores them in the event log, so you can monitor how many records are failing over time.

### 4. Quarantine Pattern (Recommended Approach)

Instead of dropping bad records or failing the whole pipeline, many teams send bad records to a separate quarantine table. This way nothing is lost and you can investigate later.

```python
from pyspark.sql.functions import col, when, current_timestamp

def apply_quality_checks(df):
    return df.withColumn(
        "quality_issue",
        when(col("order_id").isNull(), "missing_order_id")
        .when(col("order_amount") <= 0, "invalid_amount")
        .when(~col("status").isin("NEW", "SHIPPED", "DELIVERED"), "invalid_status")
        .otherwise(None)
    )

raw_df = spark.readStream.table("bronze.orders")
checked_df = apply_quality_checks(raw_df)

# Good records
good_df = checked_df.filter(col("quality_issue").isNull())
# Bad records
bad_df = checked_df.filter(col("quality_issue").isNotNull()) \
                   .withColumn("quarantine_time", current_timestamp())

# Write good data to silver
good_df.writeStream.format("delta").option("checkpointLocation", "/tmp/chk_good") \
       .toTable("silver.orders")

# Write bad data to quarantine
bad_df.writeStream.format("delta").option("checkpointLocation", "/tmp/chk_bad") \
      .toTable("quarantine.orders")
```

This pattern works well with both batch and streaming jobs.

### 5. Quality Checks Across Medallion Layers

A practical approach is to apply different levels of quality checks at each layer:

- **Bronze**: Basic checks only (schema, not null on key columns). Keep almost everything so you can reprocess if needed.
- **Silver**: Stronger checks (ranges, allowed values, uniqueness). Use drop or quarantine for bad records.
- **Gold**: Very strict checks. Prefer fail-fast so business users never see bad data.

Example uniqueness check before writing to Gold:

```python
from pyspark.sql.functions import count

duplicate_check = df.groupBy("order_id").agg(count("*").alias("cnt")) \
                    .filter(col("cnt") > 1)

if duplicate_check.count() > 0:
    raise Exception("Duplicate order_ids found. Stopping pipeline.")
```

### 6. Monitoring and Debugging

- Use DLT event log to track expectation failures over time.
- Use Delta time travel to see what the data looked like before a bad load:

```sql
SELECT * FROM silver.orders VERSION AS OF 15;
-- or
SELECT * FROM silver.orders TIMESTAMP AS OF '2023-05-10T10:00:00';
```

- Maintain a simple quality metrics table that stores daily pass/fail counts.

### Summary of Practical Techniques

| Technique                  | Best Used In     | What It Does                          | Action on Bad Data      |
|---------------------------|------------------|---------------------------------------|-------------------------|
| Schema Enforcement        | All layers       | Blocks wrong schema                   | Fails write             |
| CHECK / NOT NULL          | Silver / Gold    | Enforces business rules               | Fails write             |
| DLT Expectations          | DLT pipelines    | Declarative quality rules             | Warn / Drop / Fail      |
| Quarantine Pattern        | Batch & Streaming| Separates good and bad records        | Moves bad to side table |
| Custom Spark Checks       | Any job          | Flexible validation logic             | Your choice             |

### Final Thoughts

Data quality in a lakehouse works best when it is applied at multiple points rather than only at the end. Start with schema enforcement and simple constraints, then add DLT Expectations or a quarantine pattern for more control. The goal is to stop bad data as early as possible while still keeping enough information to debug issues later.

Once these checks are in place, your Silver and Gold tables become much more trustworthy for reporting and downstream systems.
