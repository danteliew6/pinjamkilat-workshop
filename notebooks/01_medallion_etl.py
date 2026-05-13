# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — Medallion ETL fundamentals
# MAGIC
# MAGIC **25 minutes.** Before we get to AI Functions, let's exercise the everyday data-engineering muscles every Databricks workspace runs on: **Delta Lake** + **the medallion pattern**.
# MAGIC
# MAGIC By the end of this notebook you'll have:
# MAGIC
# MAGIC - A typed, type-safe silver table built with `CREATE TABLE AS SELECT`.
# MAGIC - An **incremental upsert** run via `MERGE INTO` — the daily-batch pipeline pattern.
# MAGIC - A demonstration of **Delta time travel** (`VERSION AS OF`) for audit/regulatory rollback.
# MAGIC - A run of `OPTIMIZE … ZORDER BY` for query performance.
# MAGIC
# MAGIC > **No AI Functions are used here.** This is plain SQL + Delta — universal pipeline plumbing.

# COMMAND ----------

CATALOG = "workshop"

# COMMAND ----------

# MAGIC %md ## The medallion pattern in one slide
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
# MAGIC │   BRONZE      │    │    SILVER     │    │     GOLD      │
# MAGIC │  (raw)        │ -> │  (cleaned,    │ -> │ (business-    │
# MAGIC │  preserve     │    │   typed,      │    │  ready,       │
# MAGIC │  source-of-   │    │   conformed)  │    │  denormalized)│
# MAGIC │  truth        │    │               │    │               │
# MAGIC └───────────────┘    └───────────────┘    └───────────────┘
# MAGIC      append-only        upsert via         feeds dashboards
# MAGIC    schema-enforced     MERGE INTO          + Genie spaces
# MAGIC ```
# MAGIC
# MAGIC **Why this layering?** Each stage has different consumers and different change cadences. Bronze is your tape-recorder. Silver is your warehouse staff's working set. Gold is what the rest of the company sees.

# COMMAND ----------

# MAGIC %md ## Step 1 — Inspect what bronze actually is
# MAGIC
# MAGIC `workshop.bronze.loan_application` was created by notebook 0. It's a **Delta table**: ACID-safe, schema-enforced, with full history. Let's look under the hood.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED workshop.bronze.loan_application

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the `Provider = delta`, the storage location under `/Volumes` / managed storage, and the column types. This is *not* a CSV — it's a transactional storage format. Every change writes a new version + a JSON log line.

# COMMAND ----------

# MAGIC %md ## Step 2 — Build a typed silver table with `CREATE TABLE AS SELECT`
# MAGIC
# MAGIC The silver layer is where you **conform types, normalise codes, and derive light columns** that the rest of the warehouse can rely on. No AI yet — just SQL.

# COMMAND ----------

# Spark requires schema and AS SELECT to be in separate statements when you
# want NOT NULL + inline column comments, so we split into DDL + INSERT.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.loan_application_dim (
  application_id           BIGINT  NOT NULL  COMMENT 'Stable application key.',
  applicant_name           STRING            COMMENT 'Applicant full name.',
  gender                   STRING            COMMENT 'L (laki-laki) or P (perempuan).',
  dob                      DATE              COMMENT 'Date of birth.',
  age_years                INT               COMMENT 'Age in years derived from dob.',
  kabupaten                STRING            COMMENT 'Applicant kabupaten / city.',
  provinsi                 STRING            COMMENT 'Applicant province.',
  channel                  STRING            COMMENT 'APP / AGENT / BRANCH.',
  requested_amount_idr     BIGINT            COMMENT 'Loan amount requested in IDR.',
  tenor_months             INT               COMMENT 'Requested tenor (months).',
  monthly_existing_debt_idr BIGINT           COMMENT 'Declared monthly debt.',
  application_ts           TIMESTAMP         COMMENT 'When the application was submitted.',
  application_date         DATE              COMMENT 'Application date (derived from application_ts).'
)
USING DELTA
COMMENT 'Silver dimension table for loan applications — type-enforced, lightly derived. Updated incrementally via MERGE.'
""")

spark.sql(f"""
INSERT INTO {CATALOG}.silver.loan_application_dim
SELECT
  CAST(application_id AS BIGINT)        AS application_id,
  applicant_name,
  gender,
  CAST(dob AS DATE)                     AS dob,
  CAST(FLOOR(MONTHS_BETWEEN(current_date(), dob) / 12) AS INT) AS age_years,
  kabupaten,
  provinsi,
  channel,
  CAST(requested_amount_idr AS BIGINT)  AS requested_amount_idr,
  CAST(tenor_months AS INT)             AS tenor_months,
  CAST(monthly_existing_debt_idr AS BIGINT) AS monthly_existing_debt_idr,
  application_ts,
  CAST(application_ts AS DATE)          AS application_date
FROM {CATALOG}.bronze.loan_application
""")

spark.sql(f"SELECT COUNT(*) AS rows FROM {CATALOG}.silver.loan_application_dim").display()

# COMMAND ----------

# MAGIC %md
# MAGIC We declared the schema **and** populated it in one statement (CTAS). Column types and `NOT NULL` constraints are enforced going forward — any future MERGE/INSERT that breaks them will fail loudly.

# COMMAND ----------

# MAGIC %md ## Step 3 — Incremental upsert with `MERGE INTO`
# MAGIC
# MAGIC Real banking pipelines run nightly: *some* applications are new, *some* are updates (e.g. risk officer corrects a field). `MERGE INTO` handles both atomically.
# MAGIC
# MAGIC We'll simulate a tiny batch of "new" applications — 1 update + 2 inserts.

# COMMAND ----------

# Show the current row for application_id 100000 (we'll update it)
display(spark.sql(f"""
SELECT application_id, applicant_name, monthly_existing_debt_idr, requested_amount_idr
FROM {CATALOG}.silver.loan_application_dim
WHERE application_id IN (100000, 999001, 999002)
ORDER BY application_id
"""))

# COMMAND ----------

# Build a temp view representing the "incoming batch" — 1 update + 2 new applications.
spark.sql("""
CREATE OR REPLACE TEMP VIEW new_application_batch AS
SELECT * FROM VALUES
  (100000, 'Updated Applicant Name', 'L', DATE'1990-01-01', 35,
     'Karawang', 'Jawa Barat', 'APP',
     12000000, 12, 1500000, current_timestamp(), current_date()),
  (999001, 'Anita Wijaya',           'P', DATE'1988-04-15', 37,
     'Bekasi',  'Jawa Barat', 'BRANCH',
     25000000, 24,  500000, current_timestamp(), current_date()),
  (999002, 'Reza Maulana',           'L', DATE'1995-09-22', 30,
     'Surabaya','Jawa Timur','AGENT',
     8000000,  12,  200000, current_timestamp(), current_date())
AS t(application_id, applicant_name, gender, dob, age_years,
     kabupaten, provinsi, channel,
     requested_amount_idr, tenor_months, monthly_existing_debt_idr,
     application_ts, application_date)
""")
spark.sql("SELECT * FROM new_application_batch").display()

# COMMAND ----------

# The actual MERGE — atomic upsert.
spark.sql(f"""
MERGE INTO {CATALOG}.silver.loan_application_dim AS t
USING new_application_batch AS s
ON t.application_id = s.application_id
WHEN MATCHED THEN UPDATE SET
  applicant_name              = s.applicant_name,
  monthly_existing_debt_idr   = s.monthly_existing_debt_idr,
  requested_amount_idr        = s.requested_amount_idr,
  application_ts              = s.application_ts
WHEN NOT MATCHED THEN INSERT *
""")

# Confirm
display(spark.sql(f"""
SELECT application_id, applicant_name, monthly_existing_debt_idr, requested_amount_idr
FROM {CATALOG}.silver.loan_application_dim
WHERE application_id IN (100000, 999001, 999002)
ORDER BY application_id
"""))

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #1 — write your own MERGE
# MAGIC
# MAGIC Same idea, different batch. The temp view `your_batch` is pre-built with one row (application_id `100001`). Write a one-statement MERGE that **only updates** the existing row's `requested_amount_idr` (no INSERT clause, since we only have updates).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW your_batch AS
SELECT 100001 AS application_id, CAST(50000000 AS BIGINT) AS new_amount_idr
""")

# YOUR TURN — fill in the MERGE.
# spark.sql(f"""
# MERGE INTO {CATALOG}.silver.loan_application_dim AS t
# USING your_batch AS s
# ON t.application_id = s.application_id
# WHEN MATCHED THEN UPDATE SET
#   requested_amount_idr = ...
# """)


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

spark.sql(f"""
MERGE INTO {CATALOG}.silver.loan_application_dim AS t
USING your_batch AS s
ON t.application_id = s.application_id
WHEN MATCHED THEN UPDATE SET
  requested_amount_idr = s.new_amount_idr
""")

display(spark.sql(f"""
SELECT application_id, applicant_name, requested_amount_idr
FROM {CATALOG}.silver.loan_application_dim
WHERE application_id = 100001
"""))

# COMMAND ----------

# MAGIC %md ## Step 4 — Delta time travel
# MAGIC
# MAGIC Every MERGE / UPDATE / DELETE / INSERT writes a **new version** of the table. The old versions stay queryable until you `VACUUM` them away. This is huge for banking: regulators ask "what did this table look like on May 10?" — Delta has the answer.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workshop.silver.loan_application_dim

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the `operation` column — `CREATE TABLE AS SELECT`, then `MERGE`, then another `MERGE`. Each `version` is queryable.

# COMMAND ----------

# Compare current vs version 1 (just after the initial INSERT, before any MERGE)
display(spark.sql(f"""
SELECT 'current'    AS snapshot, applicant_name, requested_amount_idr
FROM {CATALOG}.silver.loan_application_dim
WHERE application_id = 100000

UNION ALL

SELECT 'version 1'  AS snapshot, applicant_name, requested_amount_idr
FROM {CATALOG}.silver.loan_application_dim VERSION AS OF 1
WHERE application_id = 100000
"""))

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #2 — query the table at version 2
# MAGIC
# MAGIC The history above shows the operations in order: v0 = CREATE, v1 = INSERT (all bronze rows), v2 = MERGE demo (3-row batch), v3 = MERGE YT#1 (1-row update on 100001).
# MAGIC
# MAGIC **Your task:** at version 2 — *right after the MERGE demo, before the YT#1 update* — what was `application_id = 100001`'s `requested_amount_idr`?

# COMMAND ----------

# YOUR TURN — one line, swap the VERSION AS OF.
# display(spark.sql(f"""
# SELECT applicant_name, requested_amount_idr
# FROM {CATALOG}.silver.loan_application_dim VERSION AS OF ...
# WHERE application_id = 100001
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT applicant_name, requested_amount_idr
FROM {CATALOG}.silver.loan_application_dim VERSION AS OF 2
WHERE application_id = 100001
"""))

# COMMAND ----------

# MAGIC %md ## Step 5 — `OPTIMIZE` + `ZORDER BY`
# MAGIC
# MAGIC Delta tables accumulate small files over time (one per write). `OPTIMIZE` compacts them. `ZORDER BY` reorders rows so queries filtering on the ZORDERed column scan less data.
# MAGIC
# MAGIC For banking, `application_id` is the everyday filter — perfect ZORDER candidate.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workshop.bronze.transaction ZORDER BY (application_id)

# COMMAND ----------

# MAGIC %md
# MAGIC The result row tells you how many files were combined (`numFilesRemoved`) and how many new optimised files were written (`numFilesAdded`). Subsequent `WHERE application_id = X` queries will scan less data.
# MAGIC
# MAGIC In production you'd schedule `OPTIMIZE` daily/weekly (or use Predictive Optimization on Unity Catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Notebook complete
# MAGIC
# MAGIC You exercised:
# MAGIC
# MAGIC | Concept | What you ran |
# MAGIC |---|---|
# MAGIC | Inspect Delta metadata | `DESCRIBE EXTENDED` |
# MAGIC | Build silver from bronze | `CREATE TABLE … AS SELECT` |
# MAGIC | Incremental upsert | `MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED` |
# MAGIC | Auditability | `DESCRIBE HISTORY` + `VERSION AS OF` |
# MAGIC | Performance tuning | `OPTIMIZE … ZORDER BY` |
# MAGIC
# MAGIC None of these required Mosaic AI or any paid feature — they're built into Delta + Spark SQL on every Databricks workspace, Free Edition included.
# MAGIC
# MAGIC **Next:** `02_ai_extract_kyc.py` — now that the plumbing is solid, we layer AI Functions on top to handle the messy free-text fields.
