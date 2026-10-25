---
title: Dynamic Data Masking and Row Level Security on Databricks with Immuta
date: 2023-10-24
author: Rabindra Jaiswal
categories: [Data Engineering, Databricks, Data Governance]
---

# Dynamic Data Masking and Row Level Security on Databricks with Immuta

Managing data access in a rapidly growing data team usually starts simply. You set up a few read-only groups in your database, grant access to developers and analysts, and move on. But once you start handling financial records, payment switch logs, or customer personal details at scale, this informal approach breaks down fast.

Consider a digital payments company in India handling millions of merchant settlements, RuPay card transactions, and UPI switch logs every day. The engineering team ingests all of this raw telemetry into a Databricks Delta Lake. Naturally, the operations team in Bengaluru should only look at Karnataka merchant records, the Mumbai team should only debug Maharashtra transactions, and nobody in regular business operations should see unmasked card numbers or customer mobile numbers. 

Traditionally, data platform engineers solve this by building a web of database views:

```sql
-- The classic view-sprawl nightmare
CREATE VIEW settlements_karnataka AS 
SELECT 
    txn_id, 
    CONCAT('XXXX-XXXX-', RIGHT(card_number, 4)) AS masked_card,
    amount, 
    settlement_status 
FROM raw_settlements.rupay_txns 
WHERE merchant_state = 'KA';
```

Multiply this by twenty-eight states, dozens of internal consumer roles, and hundreds of tables, and you have view sprawl. Every minor schema change breaks downstream views. Maintenance becomes a full-time chore, and sooner or later, an engineer runs a `GRANT SELECT` on the wrong underlying table.

External policy orchestration layers like Immuta handle this problem differently. Instead of duplicating data or building brittle views, they decouple policy definition from the storage layer and enforce row-level filtering and column-level dynamic masking directly at the Spark query execution layer.

Let us dig into how this integration works under the hood with Databricks, what happens to the Spark Catalyst optimizer, and the operational trade-offs you have to consider.

---

## Moving Beyond Role-Based Access to Attribute-Based Control

In traditional Role-Based Access Control (RBAC), user permissions depend strictly on their assigned database group:

$$\text{User} \longrightarrow \text{Role} \longrightarrow \text{Table Grants}$$

When you scale to hundreds of analysts across different operational branches—say, risk assessment teams in Delhi, Chennai, and Kolkata—the number of roles explodes. If an analyst handles fraud investigations across both Tamil Nadu and Kerala, you must either create a new bespoke role or add them to multiple overlapping groups that quickly become impossible to audit.

Attribute-Based Access Control (ABAC) replaces this static model. Instead of hardcoding permissions against user identities, access evaluates dynamically based on metadata attributes tied to the user session:

$$\text{User Attributes (State, Department, Clearance)} + \text{Data Attributes (Tags, Columns)} \longrightarrow \text{Query Policy}$$

In Immuta, these attributes are synced from enterprise identity providers (IdPs) like Azure Active Directory, Okta, or internal LDAP/SAML systems. When an engineer logs into Databricks through single sign-on (SSO), their identity carries along metadata tags, such as:

```json
{
  "user": "eric_sharma",
  "department": "fraud_ops",
  "assigned_states": ["MH", "KA", "DL"],
  "clearance_level": "l2"
}
```

### Dynamic Subscription Policies

Before an analyst can even touch a table, Immuta validates entitlement via a subscription policy. Instead of a database administrator manually typing `GRANT SELECT ON TABLE settlements TO user`, the policy evaluates logic against identity attributes.

If the rule says:
> "Grant access if user attribute `department` equals `fraud_ops`"

The user sees the table in their catalog automatically. If the user shifts to marketing next month, their Active Directory group updates, their attribute changes, and table access disappears without a data engineer ever touching Databricks workspace permissions.

---

## Spark Execution Mechanics: How Policy Interception Actually Works

The biggest technical question when plugging an external governance engine into Apache Spark is: **Where does policy enforcement happen?**

If it runs as a proxy outside the cluster, your network throughput tanks. If it runs inside your application code, analysts writing custom notebooks can easily bypass it.

Immuta hooks directly into the Databricks Spark runtime. When you initialize a cluster configured with Immuta, it registers custom Spark extensions and security plugins into the driver and executor JVMs.

```
+---------------------------------------------------------------+
|                      Databricks Notebook                      |
|       SELECT * FROM rupay_txns WHERE amount > 5000;           |
+---------------------------------------------------------------+
                               |
                               v
+---------------------------------------------------------------+
|                    Spark Catalyst Optimizer                   |
|                                                               |
|  1. Parsed Logical Plan                                       |
|  2. Immuta Interceptor (Resolves ABAC Policies & Tags)         |
|  3. Rewritten Logical Plan:                                   |
|     Filter(merchant_state IN ('MH', 'KA', 'DL'))              |
|     Project(txn_id, regexp_replace(card_num, ...), amount)    |
|  4. Optimized Physical Plan                                   |
+---------------------------------------------------------------+
                               |
                               v
+---------------------------------------------------------------+
|                Delta Lake Engine (I/O Read)                   |
|   - Parquet File Skipping / Partition Pruning                 |
|   - Reads only relevant state files from S3/ADLS              |
+---------------------------------------------------------------+
```

### 1. Intercepting the Catalyst Logical Plan

When a user executes a query:

```sql
SELECT txn_id, card_number, amount, merchant_state 
FROM payments_lake.rupay_settlements;
```

Spark’s Catalyst optimizer parses this SQL text into an unresolved logical plan. Before this plan is analyzed and pushed to optimization, Immuta’s security plugin intercepts the syntax tree. 

It consults the local policy cache (synchronized with the Immuta control plane) and inspects:
1. Who is running the query? (`eric_sharma`)
2. What are their session attributes? (`assigned_states = ['MH', 'KA', 'DL']`)
3. What policies apply to the source table `payments_lake.rupay_settlements`?

### 2. Injecting Filter and Project Operators

The engine rewrites the logical plan in memory before generating physical tasks.

For row-level security, Catalyst injects a logical `Filter` node above the relation scan:

```
Filter (merchant_state IN ('MH', 'KA', 'DL'))
```

For column-level security, Catalyst replaces the direct column projection node (`card_number`) with a masked expression:

```
Project [
   txn_id, 
   regexp_replace(card_number, '\\d{4}(?=\\d{4})', 'XXXX') AS card_number,
   amount, 
   merchant_state
]
```

Because this rewrite happens natively inside the Spark driver before the physical execution plan is finalized, Spark treats these injected predicates like any ordinary query filter written by the user.

---

## The Performance Impact: Pruning vs. Predicate Overhead

Rewriting queries at the plan level brings major performance advantages over old-school secure views, but it also carries real trade-offs that you need to design around.

### Partition Pruning Down to Delta Files

If your underlying Delta table is physically partitioned by `merchant_state`:

```
/mnt/datalake/rupay_settlements/merchant_state=MH/
/mnt/datalake/rupay_settlements/merchant_state=KA/
/mnt/datalake/rupay_settlements/merchant_state=TN/
```

When Immuta injects the filter:

$$\text{merchant\_state} \in (\text{'MH'}, \text{'KA'}, \text{'DL'})$$

The Catalyst optimizer pushes this predicate straight down to Delta Lake's file pruning layer. Delta reads only the metadata JSON log files for the directories corresponding to `MH`, `KA`, and `DL`. The I/O scan completely skips `TN`, `UP`, and other states. 

Here, security enforcement actually **saves compute costs and lowers latency** because the user cannot accidentally trigger a full-table scan across all partitions.

### The Trade-off: Non-Partitioned Attributes and Expression Cost

The danger arises when your row-level policy evaluates against non-partitioned or unindexed high-cardinality fields. 

Suppose the policy filters transactions based on an internal bank routing branch code (`ifsc_prefix`) that is not part of the partition strategy or liquid clustering keys. Spark must perform a full scan of the Parquet footers across all files, evaluating the filter row by row.

Similarly, complex column-masking functions carry a CPU cost:
- **Hashing functions** (e.g., `SHA-256` on an Aadhaar or PAN number) are compute-heavy. Running SHA-256 over 80 million rows on a mid-sized cluster will noticeably spike CPU utilization on your executors.
- **Regular expression evaluation** (e.g., masking the first 12 digits of a 16-digit card number) consumes significantly more CPU cycles than a simple deterministic string substring operation.

To keep query latencies predictable across notebook sessions, balance the masking complexity with actual regulatory requirements. If a basic truncation or fixed substring satisfies masking compliance, avoid deploying heavyweight nested regex patterns across tables containing hundreds of millions of records.

---

## Scaling Governance with Tag-Based Global Policies

Setting up access policies table by table works when you have ten tables. But modern lakehouses easily reach thousands of tables across ingestion, staging, curation, and aggregation layers.

Writing row-level filters and masking rules table by table quickly reintroduces the same configuration chaos we were trying to escape.

Global policies solve this by applying logic to **semantic tags** rather than physical table names:

```
+--------------------------------------------------------------------+
|                         Immuta Global Policy                       |
|   Rule: For all columns tagged 'PII.RuPayCardNumber',               |
|         apply regex mask: replace first 12 digits with 'X'         |
+--------------------------------------------------------------------+
                               |
              +----------------+----------------+
              |                                 |
              v                                 v
+-------------------------------+ +----------------------------------+
| Table: rupay_settlements      | | Table: dispute_chargebacks       |
| Column: card_num              | | Column: customer_card            |
| Tag: PII.RuPayCardNumber      | | Tag: PII.RuPayCardNumber         |
+-------------------------------+ +----------------------------------+
```

These tags can be discovered dynamically using built-in data profiling (identifying card formats matching Luhn algorithm patterns) or pulled in from enterprise data catalogs like Collibra or BigID.

When a data engineer registers a new table containing a column named `customer_card_no`, the profiling engine tags it as `PII.RuPayCardNumber`. The global masking policy immediately hooks into the column on its very first query. The engineer does not need to file a ticket with the security team to lock down the new table, closing a major window where sensitive data often leaks during ad-hoc testing.

---

## Operational Considerations and Limitations

Before rolling out native attribute-based policy enforcement on Databricks clusters, keep several practical platform factors in mind:

### 1. Cluster Initialization and Plugin Overhead

Because Immuta relies on hooks inside the Spark driver and executor runtimes, you must configure Databricks clusters with init scripts and cluster-scoped environment variables. 

- **Spark Upgrades:** Major Databricks Runtime (DBR) upgrades can occasionally break internal Spark plugin APIs. Before upgrading your production clusters from, say, DBR 14.x to 15.x, you must verify the compatibility matrix of your security vendor's plugin.
- **Cluster Startup Time:** Fetching initial policy bundles during cluster launch adds a modest overhead (usually 30 to 60 seconds) to driver boot times.

### 2. User Attribute Sync Lag

Dynamic row filtering relies on fresh user metadata from your IdP. If a support engineer in Pune gets temporarily assigned to debug transactions in Gujarat, their Active Directory group must update, and that change must propagate through the SCIM or SAML synchronization pipeline into the governance plane.

If your identity sync cycle runs on a 4-hour schedule, the engineer cannot immediately access the data. Data teams must establish an operational playbook for refreshing user attribute caches when incidents demand quick access changes.

### 3. Notebook Context Caching

Spark notebooks often maintain long-running user sessions. If a user’s policies change while their notebook cluster context is already active, Spark will not always invalidate cached DataFrames or temporary views created within that session. 

Any dataset already materialized into cluster memory (`df.cache()`) by an analyst sits outside the Catalyst evaluation path. Platform teams must set clear policies regarding caching sensitive intermediate tables in shared compute environments.

---

## Wrapping Up

Building a trustworthy data platform is not just about writing clean PySpark pipelines or optimizing Delta merge jobs; it is equally about ensuring that the right person sees only the data they are legally authorized to inspect.

Relying on hand-crafted views or manual table grants across large Databricks environments creates technical debt that slows down development and exposes the organization to massive compliance risks. 

By intercepting Spark’s Catalyst optimizer directly and applying row-filtering and column-masking rules based on user attributes and data tags, tools like Immuta decouple security from storage. Data engineers can focus on building resilient ETL pipelines, while platform administrators can manage access declaratively without getting in the way of day-to-day analytics.