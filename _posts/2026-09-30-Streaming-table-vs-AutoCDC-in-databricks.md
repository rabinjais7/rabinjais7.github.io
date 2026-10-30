---
title: streaming_table_vs_auto_cdc_in_databricks
date: 2026-09-30
author: Rabindra Jaiswal
categories: [Architecture, databricks]
---

# Databricks AUTO CDC — End-to-End Implementation and Architecture

## 1. What Problem Does AUTO CDC Solve?

In a typical data engineering project, operational databases such as SQL Server, Oracle, MySQL, or PostgreSQL contain the current state of business entities.

For example, a customer table may contain:

```text
customer_id | name | city
------------|------|--------
101         | John | Delhi
102         | Mike | Mumbai
```

Later, John moves from Delhi to Gurgaon.

The source database performs:

```sql
UPDATE customer
SET city = 'Gurgaon'
WHERE customer_id = 101;
```

The source database has changed an existing record.

If we are moving this data into Databricks, we need to know:

- Which record changed?
- Was it an INSERT, UPDATE, or DELETE?
- Which existing record should be changed?
- If multiple changes arrive for the same record, which one is the latest?

This is where CDC and AUTO CDC come into the picture.

---

# 2. The Most Important Concept

There are actually two separate problems:

### Problem 1: Capture the changes from the source

This is called **CDC extraction**.

It answers:

> What changed in the source database?

### Problem 2: Apply those changes to the Databricks target

This is what **AUTO CDC** helps with.

It answers:

> How should these changes be applied to the target table?

Therefore:

```text
CDC Extraction
      ↓
"What changed?"
      ↓
CDC records
      ↓
AUTO CDC
      ↓
"How should the target be updated?"
```

AUTO CDC does NOT normally connect directly to your operational database and discover changes by itself.

A CDC mechanism or connector is generally used first to capture the changes.

---

# 3. Complete End-to-End Architecture

A typical architecture looks like this:

```text
                    OPERATIONAL DATABASE
                SQL Server / Oracle / MySQL
                           |
                           |
                    Transaction Log
                           |
                           ↓
                    CDC Connector
                           |
              ┌────────────┴────────────┐
              |                         |
           Kafka                      ADLS
              |                         |
              └────────────┬────────────┘
                           ↓
                    Databricks Bronze
                    Streaming Table
                           |
                    CDC Event Stream
                           |
                           ↓
                       AUTO CDC
                           |
                           ↓
                    Silver Current State
                           |
                           ↓
                  Business Transformations
                           |
                           ↓
                         Gold
                           |
                           ↓
                    BI / Analytics
```

The exact CDC extraction technology can vary.

For example:

```text
SQL Server
    ↓
Debezium
    ↓
Kafka
    ↓
Databricks
```

or:

```text
SQL Server
    ↓
CDC/Replication Tool
    ↓
ADLS
    ↓
Databricks
```

Common technologies used for the CDC extraction part include CDC connectors, Debezium, Kafka Connect, Fivetran, Qlik, Informatica, and other replication/ingestion tools.

The important point is that **AUTO CDC starts after CDC data has already been captured.**

---

# 4. Step 1 — Source Operational Database

Suppose we have a customer table:

```text
customer
--------------------------------
customer_id | name | city
101         | John | Delhi
102         | Mike | Mumbai
103         | David| Pune
```

This table represents the current state of customers.

Now several things happen.

John moves to Gurgaon:

```sql
UPDATE customer
SET city = 'Gurgaon'
WHERE customer_id = 101;
```

Mike closes his account:

```sql
DELETE FROM customer
WHERE customer_id = 102;
```

Sarah becomes a new customer:

```sql
INSERT INTO customer
VALUES (104, 'Sarah', 'Chennai');
```

The source database has experienced:

```text
UPDATE
DELETE
INSERT
```

These changes need to be captured.

---

# 5. Step 2 — CDC Extraction

A CDC mechanism reads changes from the source database.

For example:

```text
SQL Server
    ↓
Transaction Log
    ↓
CDC Connector
```

The CDC connector converts the database changes into change events.

Conceptually, the CDC output could look like:

```text
customer_id | operation | name  | city     | sequence
------------|-----------|-------|----------|---------
101         | UPDATE    | John  | Gurgaon  | 10523
102         | DELETE    | Mike  | Mumbai   | 10524
104         | INSERT    | Sarah | Chennai  | 10525
```

The exact CDC message structure depends on the CDC technology being used.

But conceptually we need information such as:

```text
Business Key
Operation
Data
Sequence / Ordering Information
```

---

# 6. Step 3 — Store CDC Data in Bronze

The CDC events are normally landed into a Bronze layer.

For example:

```text
CDC Source
    ↓
Kafka / ADLS
    ↓
Databricks Bronze
```

The Bronze table could contain:

```text
customer_id | name  | city     | operation | sequence
------------|-------|----------|-----------|---------
101         | John  | Gurgaon  | UPDATE    | 10523
102         | Mike  | Mumbai   | DELETE    | 10524
104         | Sarah | Chennai  | INSERT    | 10525
```

This Bronze table can itself be a **Streaming Table**.

Why?

Because every CDC event is a new event.

Notice something important:

The Bronze table is append-only.

We don't go back and modify the Bronze record when a customer changes.

Instead, every change becomes a new CDC record.

For example:

```text
101 | John | Delhi    | INSERT | 100
101 | John | Gurgaon  | UPDATE | 101
101 | John | Bangalore | UPDATE | 102
```

All three records can remain in Bronze.

This gives us the complete history of changes.

---

# 7. Why Can Bronze Be a Streaming Table Even Though It Contains UPDATE and DELETE?

This is one of the most important concepts.

The word `UPDATE` inside the CDC data does NOT mean that we are updating the Bronze table.

For example, this CDC event:

```text
101 | UPDATE | Gurgaon
```

is itself a **new incoming event**.

Therefore Bronze can simply append it:

```text
Bronze:

Event 1 → INSERT customer 101
Event 2 → UPDATE customer 101
Event 3 → UPDATE customer 101
Event 4 → DELETE customer 101
```

The Bronze table is storing the history of changes.

So:

```text
CDC events
     ↓
append
     ↓
Bronze Streaming Table
```

The actual INSERT/UPDATE/DELETE semantics are applied later by AUTO CDC.

---

# 8. Step 4 — AUTO CDC

Now we want to convert the Bronze CDC stream into a Silver table representing the current state.

Conceptually:

```text
Bronze CDC
     |
     | INSERT / UPDATE / DELETE events
     ↓
  AUTO CDC
     |
     ↓
Silver Current-State Table
```

For example, suppose Bronze contains:

```text
customer_id | name  | city     | operation | sequence
------------|-------|----------|-----------|---------
101         | John  | Delhi    | INSERT    | 1
102         | Mike  | Mumbai   | INSERT    | 2
101         | John  | Gurgaon  | UPDATE    | 3
102         | Mike  | Mumbai   | DELETE    | 4
```

AUTO CDC processes these events.

The final Silver state becomes:

```text
customer_id | name | city
------------|------|--------
101         | John | Gurgaon
```

Customer 102 has been removed because the latest operation for that customer was DELETE.

---

# 9. AUTO CDC Implementation

A simplified implementation looks like this:

```sql
CREATE STREAMING TABLE customers;
```

Then:

```sql
CREATE FLOW customer_cdc
AS AUTO CDC INTO customers
FROM customer_cdc_bronze
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence
COLUMNS * EXCEPT (operation, sequence);
```

The exact syntax can vary with the Databricks/Lakeflow Declarative Pipelines version and implementation, but the important concepts are:

```text
FROM
KEYS
APPLY AS DELETE
SEQUENCE BY
COLUMNS
```

---

# 10. What Does KEYS Mean?

```sql
KEYS (customer_id)
```

tells AUTO CDC how to identify the entity being changed.

For example:

```text
customer_id = 101
```

means:

> This CDC event belongs to customer 101.

Without knowing the key, Databricks would not know which target record the change belongs to.

For a composite key, you can have multiple columns.

For example:

```text
KEYS (customer_id, account_id)
```

This means both columns together identify the target record.

---

# 11. What Does SEQUENCE BY Mean?

Suppose several changes arrive for the same customer:

```text
customer_id | operation | city      | sequence
------------|-----------|-----------|---------
101         | INSERT    | Delhi     | 1
101         | UPDATE    | Gurgaon   | 2
101         | UPDATE    | Bangalore | 3
```

AUTO CDC needs to know which change is the latest.

That's what:

```sql
SEQUENCE BY sequence
```

provides.

The sequence tells Databricks the order in which changes should logically be applied.

So:

```text
1 → INSERT
2 → UPDATE
3 → UPDATE
```

The final state is:

```text
101 | John | Bangalore
```

---

# 12. What Does APPLY AS DELETE Mean?

Suppose the CDC source produces:

```text
customer_id | operation
102         | DELETE
```

We need to tell AUTO CDC that this particular event represents a deletion.

For example:

```sql
APPLY AS DELETE WHEN operation = 'DELETE'
```

This means:

> When the operation column contains DELETE, remove that key from the target.

So:

```text
Bronze:

102 | DELETE

        ↓

AUTO CDC

        ↓

Silver:

Customer 102 is deleted
```

---

# 13. What Does COLUMNS EXCEPT Mean?

The CDC record contains metadata fields such as:

```text
operation
sequence
```

These are needed to process the CDC event, but they may not belong in the business table.

Therefore:

```sql
COLUMNS * EXCEPT (operation, sequence)
```

means:

> Use the business columns for the target but don't include these CDC metadata columns as target columns.

The target might therefore contain:

```text
customer_id
name
city
```

instead of:

```text
customer_id
name
city
operation
sequence
```

---

# 14. Complete Flow With Example

Let's put everything together.

## Source

```text
customer table

101 | John | Delhi
102 | Mike | Mumbai
```

## Business changes

```text
John moves to Gurgaon
Mike closes account
Sarah becomes customer
```

## CDC extraction

CDC produces:

```text
101 | UPDATE | John  | Gurgaon | 101
102 | DELETE | Mike  | Mumbai  | 102
103 | INSERT | Sarah | Chennai | 103
```

## Bronze

Bronze stores these events:

```text
customer_id | name  | city     | operation | sequence
------------|-------|----------|-----------|---------
101         | John  | Gurgaon  | UPDATE    | 101
102         | Mike  | Mumbai   | DELETE    | 102
103         | Sarah | Chennai  | INSERT    | 103
```

Bronze is append-oriented.

## AUTO CDC

AUTO CDC uses:

```text
KEYS
SEQUENCE BY
DELETE CONDITION
```

to apply the changes.

## Silver

The resulting Silver table becomes:

```text
customer_id | name  | city
------------|-------|--------
101         | John  | Gurgaon
103         | Sarah | Chennai
```

---

# 15. Why Not Just Use MERGE?

Before AUTO CDC, a common approach was to manually implement CDC using `MERGE`.

Conceptually:

```sql
MERGE INTO silver_customer target
USING bronze_cdc source
ON target.customer_id = source.customer_id

WHEN MATCHED AND source.operation = 'UPDATE'
THEN UPDATE SET *

WHEN MATCHED AND source.operation = 'DELETE'
THEN DELETE

WHEN NOT MATCHED AND source.operation = 'INSERT'
THEN INSERT *;
```

This can work, but you then have to manage:

- Matching logic
- Ordering
- Deletes
- Multiple changes for the same key
- Streaming/incremental processing
- CDC-specific behavior

AUTO CDC provides a higher-level CDC processing pattern for this use case.

---

# 16. The Most Important Architecture Pattern

A very common mental model is:

```text
                  SOURCE
                    |
                    ↓
             Operational DB
                    |
                    ↓
              CDC Extraction
                    |
                    ↓
            Kafka / ADLS / etc.
                    |
                    ↓
              BRONZE LAYER
           Append-only CDC events
                    |
                    ↓
                 AUTO CDC
                    |
                    ↓
              SILVER LAYER
           Current-state entities
                    |
                    ↓
           Business Transformations
                    |
                    ↓
                GOLD LAYER
```

---

# 17. Why Streaming Table and AUTO CDC Are Used Together

This is where the concepts connect.

### Bronze

CDC events arrive continuously.

Therefore:

```text
CDC events
    ↓
Streaming Table
```

Because every CDC event is a new event.

### Silver

We want the latest state of each entity.

Therefore:

```text
Bronze CDC
    ↓
AUTO CDC
    ↓
Current-state Silver
```

So a single architecture can use both:

```text
CDC Source
    ↓
Streaming Table
    ↓
AUTO CDC
    ↓
Silver
```

There is no contradiction.

The Bronze layer is append-only even though the records describe INSERT/UPDATE/DELETE operations.

AUTO CDC applies those operations to the Silver target.

---

# 18. Real-World Example

Suppose a bank has:

```text
Customer DB
Account DB
Transaction DB
```

### Customer DB

Customers can be updated:

```text
Address changed
Phone changed
Customer status changed
```

Therefore:

```text
Customer DB
    ↓
CDC
    ↓
Bronze Streaming Table
    ↓
AUTO CDC
    ↓
Silver Customer
```

Silver represents the current customer state.

### Transaction DB

Transactions are generally new business events:

```text
Transaction 1001
Transaction 1002
Transaction 1003
Transaction 1004
```

Therefore:

```text
Transaction Source
    ↓
Streaming
    ↓
Bronze Streaming Table
```

There may be no need for AUTO CDC if the transaction data is genuinely append-only.

---

# 19. Streaming Table vs AUTO CDC in This Architecture

The distinction can now be understood clearly.

### Streaming Table

The question is:

> How do I continuously process incoming records?

Example:

```text
Kafka
 ↓
Streaming Table
```

Each incoming record is treated as a new event.

### AUTO CDC

The question is:

> How do I apply change events to maintain the current state of an entity?

Example:

```text
CDC Stream
    ↓
AUTO CDC
    ↓
Current State
```

Therefore they solve different problems.

---

# 20. The Key Interview Explanation

If asked:

> "How does AUTO CDC work in Databricks?"

A good answer is:

> First, CDC is captured from the source operational database using a CDC or replication mechanism. Those changes are usually landed in a Bronze streaming table as append-only CDC events. AUTO CDC then consumes that CDC stream and applies INSERT, UPDATE, and DELETE operations to the Silver target. The business key identifies the target record, while the sequence column determines the correct order of changes. This allows Silver to represent the current state of the source entity without manually implementing all the CDC MERGE logic.

---

# 21. Final Mental Model

Remember these three layers:

```text
SOURCE
"What changed?"
       ↓
      CDC
       ↓
BRONZE
"Store every change event"
       ↓
  Streaming Table
       ↓
SILVER
"Apply those changes"
       ↓
   AUTO CDC
       ↓
"Maintain current state"
```

Or even more simply:

```text
CDC = Capture the changes

Streaming Table = Continuously store/process incoming events

AUTO CDC = Apply those changes to the target

Silver = Current state of the business entity
```

The most important distinction is:

> **CDC extraction and AUTO CDC are not the same thing. CDC extraction captures changes from the source; AUTO CDC applies those captured changes to a Databricks target table.**