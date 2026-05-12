# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 — Applicant 360 + foundational data engineering
# MAGIC
# MAGIC **25 minutes.** The first three notebooks gave Genie one wide view (`vw_application_decisioning`). That works, but Genie sees nothing about *behaviour over time* — salary consistency, expense ratios, transaction patterns. This notebook fixes that.
# MAGIC
# MAGIC ## What we'll build
# MAGIC
# MAGIC | Object | Layer | Purpose |
# MAGIC |---|---|---|
# MAGIC | `silver.transaction_features` | silver | One row per applicant. Aggregations + window-function-derived features over the 12-month transaction history. |
# MAGIC | `silver.call_summary` | silver | One row per applicant. Counts + sentiment-bucket distribution from call notes. |
# MAGIC | `gold.vw_applicant_360` | gold | One row per applicant. Everything Genie needs to talk about an applicant — joined from decisioning + tx features + call summary. |
# MAGIC
# MAGIC ## Concepts we'll teach
# MAGIC
# MAGIC - **Window functions** (`OVER PARTITION BY`) for time-series features.
# MAGIC - **CTEs** for legible multi-step transformations.
# MAGIC - **Percentiles** via `approx_percentile` for distribution features (cheaper than `PERCENTILE_CONT` on Spark).
# MAGIC - **Data-quality checks** asserting row counts, null rates, and value ranges.
# MAGIC - **Column COMMENTs as Genie metadata** — Genie reads these to disambiguate fields.

# COMMAND ----------

CATALOG = "workshop"

# COMMAND ----------

# MAGIC %md ## Step 1 — Per-month transaction aggregates (window functions)
# MAGIC
# MAGIC We bucket each transaction into its calendar month, then compute per-applicant-per-month totals: salary credits, non-salary credits, total debits. This is the kind of intermediate aggregate you'd materialise in a real bronze→silver pipeline.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.tx_monthly AS
SELECT
  application_id,
  DATE_TRUNC('MONTH', transaction_ts)                                              AS tx_month,
  SUM(CASE WHEN transaction_type = 'SALARY_IN' THEN amount_idr ELSE 0 END)         AS salary_credit_idr,
  SUM(CASE WHEN transaction_type IN ('TRANSFER_IN') THEN amount_idr ELSE 0 END)    AS nonsalary_credit_idr,
  SUM(CASE WHEN amount_idr < 0 THEN -amount_idr ELSE 0 END)                        AS total_debit_idr,
  COUNT(CASE WHEN transaction_type = 'SALARY_IN' THEN 1 END)                       AS n_salary_credits,
  COUNT(*)                                                                          AS n_transactions
FROM {CATALOG}.bronze.transaction
GROUP BY application_id, DATE_TRUNC('MONTH', transaction_ts)
""")

display(spark.sql(f"""
SELECT * FROM {CATALOG}.silver.tx_monthly
WHERE application_id IN (SELECT application_id FROM {CATALOG}.silver.application_kyc LIMIT 2)
ORDER BY application_id, tx_month
"""))

# COMMAND ----------

# MAGIC %md ## Step 2 — Roll the monthlies up into per-applicant features
# MAGIC
# MAGIC Here we use `approx_percentile` to capture distribution shape (median, p25, p75) and `STDDEV / AVG` to derive a *salary consistency score* — high std-dev relative to mean = unstable income.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.transaction_features (
  application_id              COMMENT 'Application identifier (foreign key).',
  n_months_observed           COMMENT 'Count of distinct months in the 12-month transaction window.',
  total_salary_idr            COMMENT 'Sum of SALARY_IN credits over the observation window, in IDR.',
  total_nonsalary_credit_idr  COMMENT 'Sum of non-salary credits (transfers in) over the observation window.',
  total_debit_idr             COMMENT 'Sum of debits (negative amounts) over the observation window.',
  avg_monthly_salary_idr      COMMENT 'Mean monthly salary credit. Compare to extracted_income_idr to detect over/understatement.',
  median_monthly_salary_idr   COMMENT '50th percentile monthly salary credit (approx_percentile).',
  p25_monthly_salary_idr      COMMENT '25th percentile monthly salary credit.',
  p75_monthly_salary_idr      COMMENT '75th percentile monthly salary credit.',
  salary_stddev_idr           COMMENT 'Std deviation of monthly salary credits.',
  salary_consistency_score    COMMENT 'salary_stddev / avg_monthly_salary. 0 = perfectly consistent; higher = volatile. NULL if avg is 0.',
  avg_monthly_debit_idr       COMMENT 'Mean monthly debit.',
  expense_to_income_ratio     COMMENT 'avg_monthly_debit / avg_monthly_salary. NULL when salary is 0. Watch >1.0 = spending more than earning.',
  n_total_transactions        COMMENT 'Total transactions across all months.'
)
COMMENT 'Per-applicant transaction features derived from 12 months of bank-statement data. Used by gold.vw_applicant_360 and Genie.'
AS
WITH m AS (
  SELECT * FROM {CATALOG}.silver.tx_monthly
),
rolled AS (
  SELECT
    application_id,
    COUNT(DISTINCT tx_month)                                            AS n_months_observed,
    SUM(salary_credit_idr)                                              AS total_salary_idr,
    SUM(nonsalary_credit_idr)                                           AS total_nonsalary_credit_idr,
    SUM(total_debit_idr)                                                AS total_debit_idr,
    AVG(salary_credit_idr)                                              AS avg_monthly_salary_idr,
    APPROX_PERCENTILE(salary_credit_idr, 0.5)                           AS median_monthly_salary_idr,
    APPROX_PERCENTILE(salary_credit_idr, 0.25)                          AS p25_monthly_salary_idr,
    APPROX_PERCENTILE(salary_credit_idr, 0.75)                          AS p75_monthly_salary_idr,
    STDDEV(salary_credit_idr)                                           AS salary_stddev_idr,
    AVG(total_debit_idr)                                                AS avg_monthly_debit_idr,
    SUM(n_transactions)                                                 AS n_total_transactions
  FROM m
  GROUP BY application_id
)
SELECT
  application_id,
  n_months_observed,
  CAST(total_salary_idr           AS BIGINT) AS total_salary_idr,
  CAST(total_nonsalary_credit_idr AS BIGINT) AS total_nonsalary_credit_idr,
  CAST(total_debit_idr            AS BIGINT) AS total_debit_idr,
  CAST(avg_monthly_salary_idr     AS BIGINT) AS avg_monthly_salary_idr,
  CAST(median_monthly_salary_idr  AS BIGINT) AS median_monthly_salary_idr,
  CAST(p25_monthly_salary_idr     AS BIGINT) AS p25_monthly_salary_idr,
  CAST(p75_monthly_salary_idr     AS BIGINT) AS p75_monthly_salary_idr,
  CAST(salary_stddev_idr          AS BIGINT) AS salary_stddev_idr,
  CASE WHEN avg_monthly_salary_idr > 0
    THEN ROUND(salary_stddev_idr / avg_monthly_salary_idr, 4)
    ELSE NULL END AS salary_consistency_score,
  CAST(avg_monthly_debit_idr AS BIGINT) AS avg_monthly_debit_idr,
  CASE WHEN avg_monthly_salary_idr > 0
    THEN ROUND(avg_monthly_debit_idr / avg_monthly_salary_idr, 3)
    ELSE NULL END AS expense_to_income_ratio,
  n_total_transactions
FROM rolled
""")

display(spark.sql(f"SELECT * FROM {CATALOG}.silver.transaction_features ORDER BY application_id LIMIT 10"))

# COMMAND ----------

# MAGIC %md ## (Presenter step) Wrap with a view that adds `savings_velocity_idr`
# MAGIC
# MAGIC Net monthly cash flow = `(salary + other credits − debits) / months`. Positive = accumulating; negative = bleeding cash. This is the kind of derived feature gold views typically expose — we wrap rather than re-materialise.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.v_transaction_features (
  application_id, n_months_observed, total_salary_idr, total_nonsalary_credit_idr,
  total_debit_idr, avg_monthly_salary_idr, median_monthly_salary_idr,
  p25_monthly_salary_idr, p75_monthly_salary_idr, salary_stddev_idr,
  salary_consistency_score, avg_monthly_debit_idr, expense_to_income_ratio,
  n_total_transactions,
  savings_velocity_idr        COMMENT 'Per-month net cash flow = (salary + non-salary credits - debits) / n_months. Positive = accumulating savings; negative = spending faster than earning.'
)
AS
SELECT *,
  CASE WHEN n_months_observed > 0
    THEN CAST((total_salary_idr + total_nonsalary_credit_idr - total_debit_idr)
              * 1.0 / n_months_observed AS BIGINT)
    ELSE NULL END AS savings_velocity_idr
FROM {CATALOG}.silver.transaction_features
""")

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #1 — find the worst spenders
# MAGIC
# MAGIC Use `v_transaction_features` to list the top 10 applicants by **lowest** `savings_velocity_idr` (i.e., the biggest cash bleeders). Just one `SELECT ... ORDER BY ... LIMIT`.

# COMMAND ----------

# YOUR TURN — write one SELECT
# display(spark.sql(f"""
# SELECT application_id, avg_monthly_salary_idr, avg_monthly_debit_idr, savings_velocity_idr
# FROM {CATALOG}.silver.v_transaction_features
# ORDER BY ...
# LIMIT 10
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT application_id, avg_monthly_salary_idr, avg_monthly_debit_idr, savings_velocity_idr
FROM {CATALOG}.silver.v_transaction_features
WHERE savings_velocity_idr IS NOT NULL
ORDER BY savings_velocity_idr ASC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md ## Step 3 — Call-note summary per applicant

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.call_summary (
  application_id           COMMENT 'Application identifier.',
  total_calls              COMMENT 'Total call-center verification notes.',
  positive_calls           COMMENT 'Notes scored positive by ai_analyze_sentiment.',
  neutral_calls            COMMENT 'Notes scored neutral.',
  negative_calls           COMMENT 'Notes scored negative — these are the risk-signal calls.',
  first_call_ts            COMMENT 'Timestamp of the first call note.',
  last_call_ts             COMMENT 'Timestamp of the most recent call note.',
  has_negative_call        COMMENT 'TRUE if at least one negative-sentiment call exists.'
)
COMMENT 'Per-applicant call-center summary derived from ai_analyze_sentiment results.'
AS
SELECT
  application_id,
  COUNT(*)                                              AS total_calls,
  SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_calls,
  SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neutral_calls,
  SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_calls,
  MIN(note_ts)                                          AS first_call_ts,
  MAX(note_ts)                                          AS last_call_ts,
  SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) > 0 AS has_negative_call
FROM {CATALOG}.silver.call_note_sentiment
GROUP BY application_id
""")

# COMMAND ----------

# MAGIC %md ## Step 4 — Build `gold.vw_applicant_360`
# MAGIC
# MAGIC Wide, denormalized, governance-friendly. Every column has a COMMENT so Genie understands what it is. This is the second table we'll expose in the Genie space (alongside `vw_application_decisioning`).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.vw_applicant_360 (
  application_id              COMMENT 'Application identifier (one row per applicant).',
  applicant_name              COMMENT 'Applicant full name.',
  age_years                   COMMENT 'Age in years.',
  gender                      COMMENT 'L (laki-laki) or P (perempuan).',
  kabupaten                   COMMENT 'Applicant kabupaten / city.',
  provinsi                    COMMENT 'Applicant province.',
  channel                     COMMENT 'Application channel: APP, AGENT, or BRANCH.',
  application_ts              COMMENT 'When the application was submitted.',
  requested_amount_idr        COMMENT 'Loan amount requested (IDR).',
  tenor_months                COMMENT 'Requested tenor in months.',
  monthly_existing_debt_idr   COMMENT 'Existing monthly debt obligations declared on the form.',
  occupation_category         COMMENT 'AI-extracted occupation category (Bahasa/English).',
  employment_type             COMMENT 'AI-extracted employment type (tetap, kontrak, freelance, etc.).',
  employment_stability        COMMENT 'ai_classify result: stable / variable / informal.',
  declared_income_idr         COMMENT 'AI-extracted monthly income (IDR) from application free-text.',
  observed_avg_salary_idr     COMMENT 'Mean monthly SALARY_IN amount observed from 12 months of bank-statement transactions.',
  observed_median_salary_idr  COMMENT 'Median monthly SALARY_IN amount (approx).',
  salary_consistency_score    COMMENT 'STDDEV / mean of monthly salary credits. >0.30 typically indicates variable income; >0.60 strongly variable.',
  expense_to_income_ratio     COMMENT 'Monthly debits / monthly salary. >1.0 means the applicant spends more than they earn.',
  savings_velocity_idr        COMMENT 'Average monthly net cash flow (IDR). Negative = bleeding cash.',
  declared_vs_observed_pct    COMMENT 'declared_income_idr / observed_avg_salary_idr × 100. <80% suggests under-disclosure; >120% suggests over-disclosure.',
  total_calls                 COMMENT 'Number of call-center verification notes.',
  negative_calls              COMMENT 'Number of negative-sentiment calls.',
  has_negative_call           COMMENT 'TRUE if any call was negative-sentiment.',
  risk_score                  COMMENT 'Composite risk score 0-100 from decisioning view. Higher = lower risk.',
  decision                    COMMENT 'APPROVE / REVIEW / REJECT.',
  decision_reason_codes       COMMENT 'Array of reason codes that fired for this applicant.'
)
COMMENT 'Applicant 360 — one row per applicant joining decisioning, transaction features, and call summary. Second main view for the Genie space. Use this for behavioural / financial / declared-vs-observed questions; use vw_application_decisioning for raw rules-and-decision questions.'
AS
SELECT
  d.application_id,
  d.applicant_name,
  d.age_years, d.gender,
  d.kabupaten, d.provinsi,
  d.channel,
  d.application_ts,
  d.requested_amount_idr, d.tenor_months,
  d.monthly_existing_debt_idr,
  d.occupation_category,
  d.employment_type,
  d.employment_stability,
  d.extracted_income_idr        AS declared_income_idr,
  tf.avg_monthly_salary_idr     AS observed_avg_salary_idr,
  tf.median_monthly_salary_idr  AS observed_median_salary_idr,
  tf.salary_consistency_score,
  tf.expense_to_income_ratio,
  tf.savings_velocity_idr,
  CASE WHEN tf.avg_monthly_salary_idr > 0
    THEN ROUND(d.extracted_income_idr * 100.0 / tf.avg_monthly_salary_idr, 1)
    ELSE NULL END AS declared_vs_observed_pct,
  cs.total_calls,
  cs.negative_calls,
  cs.has_negative_call,
  d.risk_score,
  d.decision,
  d.decision_reason_codes
FROM {CATALOG}.gold.vw_application_decisioning d
LEFT JOIN {CATALOG}.silver.v_transaction_features tf USING (application_id)
LEFT JOIN {CATALOG}.silver.call_summary           cs USING (application_id)
""")

display(spark.sql(f"""
SELECT applicant_name, kabupaten, declared_income_idr, observed_avg_salary_idr,
       declared_vs_observed_pct, salary_consistency_score, expense_to_income_ratio,
       savings_velocity_idr, decision
FROM {CATALOG}.gold.vw_applicant_360
WHERE decision IN ('REVIEW','REJECT')
ORDER BY declared_vs_observed_pct DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md ## Step 5 — Data-quality checks
# MAGIC
# MAGIC Cheap defensive assertions. In a real pipeline you'd add expectations via Lakeflow / DLT — for the workshop we'll just `assert` directly.

# COMMAND ----------

# Check 1: row count matches the upstream sample
n_360 = spark.table(f"{CATALOG}.gold.vw_applicant_360").count()
n_dec = spark.table(f"{CATALOG}.gold.vw_application_decisioning").count()
print(f"applicant_360 rows: {n_360}   decisioning rows: {n_dec}")
assert n_360 == n_dec, "Row counts diverged — left join lost rows?"

# Check 2: salary_consistency_score is bounded
worst = spark.sql(f"""
SELECT MIN(salary_consistency_score), MAX(salary_consistency_score),
       AVG(salary_consistency_score)
FROM {CATALOG}.gold.vw_applicant_360
WHERE salary_consistency_score IS NOT NULL
""").collect()[0]
print(f"salary_consistency_score: min={worst[0]:.3f}  max={worst[1]:.3f}  avg={worst[2]:.3f}")
assert 0 <= worst[0], "Consistency score went negative — bug in formula"

print("\n✓ Data-quality checks pass.")

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #2 — flag the over-declarers
# MAGIC
# MAGIC `declared_vs_observed_pct` compares what the applicant *said* on the form vs what their bank-statement transactions actually show. **Over 200 means they declared 2× or more their observed salary** — high-priority verification cohort.
# MAGIC
# MAGIC Write one `SELECT` over `gold.vw_applicant_360` that returns them, with their decision, sorted by the worst offenders first.

# COMMAND ----------

# YOUR TURN
# display(spark.sql(f"""
# SELECT ...
# FROM {CATALOG}.gold.vw_applicant_360
# WHERE ...
# ORDER BY ...
# LIMIT 10
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT applicant_name, kabupaten,
       declared_income_idr, observed_avg_salary_idr, declared_vs_observed_pct,
       decision
FROM {CATALOG}.gold.vw_applicant_360
WHERE declared_vs_observed_pct > 200
ORDER BY declared_vs_observed_pct DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeaway:** the value of joining transaction features into a gold view is that *cohort discovery* becomes a one-line query. No model needed for this signal — just structured columns the AI Functions populated upstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Notebook complete
# MAGIC
# MAGIC New objects:
# MAGIC
# MAGIC - `workshop.silver.tx_monthly`
# MAGIC - `workshop.silver.transaction_features`
# MAGIC - `workshop.silver.v_transaction_features` (adds `savings_velocity_idr`)
# MAGIC - `workshop.silver.call_summary`
# MAGIC - **`workshop.gold.vw_applicant_360`** — the second view we'll expose to Genie next.
# MAGIC
# MAGIC **Next:** `05_risk_genie.py` — register both views in Genie, add certified SQL examples, and benchmark.
