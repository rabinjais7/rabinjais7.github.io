---
title: The Small Files Problem in Apache Spark Why It Happens and How to Fix It
date: 2020-07-18
author: Rabindra Jaiswal
categories: 
  - Data Engineering
  - Data Architecture
---

## The Small Files Problem in Apache Spark: Why It Happens and How to Fix It

If you have worked with Spark on HDFS, S3, or any data lake for a while, you have almost certainly run into the small files problem. Jobs that used to be fast suddenly become slow, listing directories takes forever, and the NameNode (or S3 LIST calls) starts to struggle. 

This post explains what the small files problem actually is, why Spark creates it so easily, and the practical ways to fix it.

### What Is the Small Files Problem?

In distributed storage systems, each file has metadata associated with it. On HDFS this metadata lives in the NameNode. On object stores like S3, every file listing and open operation has a cost. When you have millions of tiny files (a few MB or less), two things happen:

1. **Metadata overhead explodes** — listing a directory or opening files becomes expensive.
2. **Processing becomes inefficient** — Spark creates one task per file (or per split). Thousands of tiny tasks mean huge scheduling overhead, poor I/O utilization, and under-used executors.

The ideal file size for most analytical workloads is roughly **128 MB to 1 GB**. Files much smaller than that waste resources; files much larger than that can cause memory pressure or reduce parallelism.

### Why Spark Creates Small Files So Easily

Spark writes one output file per partition (task) by default. Several common patterns produce too many partitions:

- High parallelism settings (`spark.sql.shuffle.partitions` defaulted to 200, and many people set it even higher).
- Frequent small writes (streaming micro-batches, frequent incremental loads).
- Over-partitioning with high-cardinality columns (`partitionBy` on user_id, session_id, etc.).
- Writing after filters or aggregations that leave many partitions almost empty.
- Each task writing into every partition directory when you use `partitionBy`.

The result is thousands or millions of small Parquet/ORC files that make subsequent reads painful.

### How to Fix It

#### 1. Control the Number of Output Files with `coalesce` or `repartition`

```python
# Reduce to a reasonable number of files before writing
df.coalesce(20).write.mode("overwrite").parquet("s3://bucket/output/")

# Or use repartition when you also want to rebalance data
df.repartition(50).write.mode("overwrite").parquet("s3://bucket/output/")
```

**When to use which:**
- Prefer **`coalesce(n)`** when you only need to reduce the number of partitions (cheaper — no full shuffle).
- Use **`repartition(n)`** when partitions are skewed or you want even distribution (costs a shuffle).

You can also use SQL hints (Spark 2.4+):

```sql
INSERT OVERWRITE TABLE target
SELECT /*+ COALESCE(20) */ * FROM source;
```

**Rule of thumb:** Aim for output files in the 128 MB – 1 GB range. Calculate roughly:

```
number_of_files ≈ total_data_size / target_file_size
```

#### 2. Be Careful with `partitionBy`

```python
# This can create many small files (one per task × number of partitions)
df.write.partitionBy("date", "region").parquet("s3://bucket/output/")

# Better: repartition by the partition columns first
df.repartition("date", "region") \
  .write \
  .partitionBy("date", "region") \
  .parquet("s3://bucket/output/")
```

Also avoid partitioning on high-cardinality columns.

#### 3. Use `maxRecordsPerFile` (or similar)

```python
df.write \
  .option("maxRecordsPerFile", 1000000) \
  .parquet("s3://bucket/output/")
```

This prevents individual files from becoming too large while still controlling the overall number.

#### 4. Compaction Jobs (Periodic Rewrite)

For tables that already have many small files, run a periodic compaction job:

```python
# Simple compaction
spark.read.parquet("s3://bucket/table/") \
  .coalesce(50) \
  .write \
  .mode("overwrite") \
  .parquet("s3://bucket/table_compacted/")
```

Then swap the paths (or use a table format that supports atomic swaps).

#### 5. Delta Lake OPTIMIZE 

If you are using Delta Lake (open-sourced in 2019 and already popular on Databricks by 2020), the `OPTIMIZE` command is the cleanest way to compact files:

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://bucket/delta_table")
delta_table.optimize().executeCompaction()
```

Or in SQL:

```sql
OPTIMIZE delta.`s3://bucket/delta_table`;
-- Or only specific partitions
OPTIMIZE delta.`s3://bucket/delta_table` WHERE date = '2020-10-01';
```

Delta Lake’s OPTIMIZE uses bin-packing to combine small files into larger ones (targeting ~1 GB by default). This was one of the strongest practical reasons many teams adopted Delta Lake around that time.

### Streaming Considerations

Streaming jobs (Structured Streaming) are especially prone to small files because each micro-batch writes new files.

Practical approaches:
- Increase the trigger interval so each micro-batch processes more data.
- Use `foreachBatch` and apply `coalesce`/`repartition` inside the batch function before writing.
- Run a separate compaction job on the output path periodically.
- Prefer Delta Lake as the sink so you can later run `OPTIMIZE` easily.

### Monitoring and Prevention

- Check the number of files and average file size regularly (using `spark.read.parquet(...).inputFiles.length` or storage listing).
- Set up alerts when the number of files in a partition exceeds a threshold.
- Document target file sizes in your team’s standards.
- Prefer writing larger batches over many tiny ones when latency allows it.

### Summary

| Approach                        | Best For                          | Cost          | Notes                          |
|--------------------------------|-----------------------------------|---------------|--------------------------------|
| `coalesce(n)` before write     | Reducing files, balanced data     | Low           | No full shuffle                |
| `repartition(n)` before write  | Skewed data or rebalancing        | Higher        | Full shuffle                   |
| Careful `partitionBy`          | Hive-style partitioning           | Medium        | Repartition by key first       |
| Periodic compaction job        | Existing tables with small files  | Medium        | Run off-peak                   |
| Delta Lake `OPTIMIZE`          | Delta tables                      | Medium        | Cleanest long-term solution    |

The small files problem is almost inevitable if you do not design for it, but it is also very solvable. Controlling the number of partitions before you write, avoiding over-partitioning, and running compaction (or using Delta Lake’s OPTIMIZE).

Once you get file sizes into a healthy range, both job runtime and query performance improve significantly — and the operations team stops getting paged about slow directory listings.
