# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Silver → Gold decisioning layer
# MAGIC
# MAGIC **25 minutes.** Combine the AI signals with hard banking rules into one verdict per applicant.
# MAGIC
# MAGIC Out: **`workshop.gold.vw_application_decisioning`** — one row per applicant with `risk_score`, `decision`, and `decision_reason_codes` (an ARRAY). This is what the dashboard + Genie will read.

# COMMAND ----------

CATALOG = "workshop"

# COMMAND ----------

# MAGIC %md ## The rule set we'll encode
# MAGIC
# MAGIC | Rule | Severity | Reason code |
# MAGIC |---|---|---|
# MAGIC | Age outside 21–60 | hard reject | `AGE_OUT_OF_RANGE` |
# MAGIC | Estimated DBR > 50% | hard reject | `HIGH_DBR` |
# MAGIC | NIK on blacklist | hard reject | `BLACKLIST_MATCH` |
# MAGIC | Loan-to-income > 24 months of income | review | `LOAN_TO_INCOME_HIGH` |
# MAGIC | `employment_stability = informal` | review | `INFORMAL_EMPLOYMENT` |
# MAGIC | `employment_stability = variable` | review | `VARIABLE_INCOME` |
# MAGIC | `sentiment_aggregate = has_negative` | review | `NEGATIVE_CALL_SENTIMENT` |
# MAGIC | `purpose = debt_consolidation` | watch | `REFINANCE_PURPOSE` |
# MAGIC | `urgency = urgent` | watch | `URGENT_REQUEST` |
# MAGIC
# MAGIC `risk_score` starts at 100 and decrements by severity. Decision bands: `APPROVE` ≥ 70, `REVIEW` 40–69, `REJECT` < 40 (or any hard-reject reason).

# COMMAND ----------

# MAGIC %md ## A tiny synthetic blacklist
# MAGIC
# MAGIC We seed a handful of NIKs from the applicant population so the blacklist rule actually fires for ~1% of rows. Real banks would join against an OJK / internal AML list.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.nik_blacklist AS
SELECT nik_from_form AS nik, 'AML_ALERT' AS reason, current_date() AS added_on
FROM {CATALOG}.silver.application_kyc
ORDER BY nik_from_form
LIMIT 5
""")
spark.sql(f"SELECT * FROM {CATALOG}.silver.nik_blacklist").display()

# COMMAND ----------

# MAGIC %md ## Build the gold decisioning view

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.vw_application_decisioning (
  application_id          COMMENT 'Unique application identifier. Example 100123.',
  applicant_name          COMMENT 'Applicant full name (Bahasa/Indonesian).',
  age_years               COMMENT 'Age at application in years.',
  gender                  COMMENT 'L (laki-laki) or P (perempuan).',
  kecamatan               COMMENT 'Applicant kecamatan (sub-district).',
  kabupaten               COMMENT 'Applicant kabupaten / city.',
  provinsi                COMMENT 'Applicant province. Example: DKI Jakarta, Jawa Barat, Banten.',
  application_ts          COMMENT 'When the application was submitted.',
  channel                 COMMENT 'How the application arrived: APP, AGENT, or BRANCH.',
  requested_amount_idr    COMMENT 'Loan amount requested in Indonesian Rupiah.',
  tenor_months            COMMENT 'Requested tenor in months: 6, 12, 18, 24, or 36.',
  extracted_income_idr    COMMENT 'Monthly income (IDR) extracted from the Bahasa occupation free-text by ai_extract.',
  monthly_existing_debt_idr COMMENT 'Existing monthly debt obligations declared on the form.',
  dbr_pct                 COMMENT 'Debt Burden Ratio = monthly_existing_debt / extracted_income, as a percentage.',
  loan_to_income_months   COMMENT 'requested_amount / extracted_income, expressed in months of income.',
  occupation_category     COMMENT 'AI-extracted occupation category (free-form).',
  employer_sector         COMMENT 'AI-extracted employer sector (free-form).',
  employment_type         COMMENT 'AI-extracted employment type. Examples: tetap, kontrak, freelance, harian.',
  employment_stability    COMMENT 'ai_classify result: stable, variable, or informal.',
  purpose_category        COMMENT 'AI-extracted loan purpose category. Examples: renovation, business, debt_consolidation, medical.',
  purpose_urgency         COMMENT 'AI-extracted purpose urgency: urgent or normal.',
  total_calls             COMMENT 'Number of call-center verification notes for this applicant.',
  negative_calls          COMMENT 'Number of call notes scored as negative by ai_analyze_sentiment.',
  sentiment_aggregate     COMMENT 'Aggregate sentiment label: has_negative, positive_only, neutral_only, or no_calls.',
  decision_reason_codes   COMMENT 'Array of reason codes that fired for this applicant. Example: [HIGH_DBR, NEGATIVE_CALL_SENTIMENT].',
  risk_score              COMMENT 'Composite risk score 0-100. Higher = lower risk.',
  decision                COMMENT 'Final decision: APPROVE, REVIEW, or REJECT.'
)
COMMENT 'Gold decisioning view — one row per applicant. Combines AI-Function-derived signals with hard banking rules (DBR, age, blacklist). Primary view for the Risk Officer dashboard and Genie space. PinjamKilat / Bank Demo Sejahtera (Indonesian digital lender, synthetic data).'
AS
WITH base AS (
  SELECT
    r.application_id,
    r.applicant_name,
    CAST(FLOOR(MONTHS_BETWEEN(current_date(), a.dob_from_form) / 12) AS INT) AS age_years,
    a.gender_from_form    AS gender,
    r.kabupaten, r.provinsi,
    a.kecamatan,
    r.application_ts,
    a.channel,
    r.requested_amount_idr, r.tenor_months,
    r.extracted_income_idr,
    r.monthly_existing_debt_idr,
    CASE WHEN r.extracted_income_idr > 0
      THEN ROUND(r.monthly_existing_debt_idr * 100.0 / r.extracted_income_idr, 1)
      ELSE NULL END AS dbr_pct,
    CASE WHEN r.extracted_income_idr > 0
      THEN ROUND(r.requested_amount_idr * 1.0 / r.extracted_income_idr, 1)
      ELSE NULL END AS loan_to_income_months,
    r.extracted_occupation_category AS occupation_category,
    r.extracted_employer_sector     AS employer_sector,
    r.extracted_employment_type     AS employment_type,
    r.employment_stability,
    r.extracted_purpose_category    AS purpose_category,
    r.extracted_purpose_urgency     AS purpose_urgency,
    r.total_calls, r.negative_calls, r.sentiment_aggregate,
    -- Blacklist match
    CASE WHEN b.nik IS NOT NULL THEN true ELSE false END AS is_blacklisted
  FROM {CATALOG}.silver.v_application_risk_signals r
  JOIN {CATALOG}.silver.application_kyc a USING (application_id)
  LEFT JOIN {CATALOG}.silver.nik_blacklist b
    ON a.nik_from_form = b.nik
),
scored AS (
  SELECT *,
    -- Reason codes — collect each firing rule (FILTER removes NULL slots;
    -- ARRAY_REMOVE(arr, NULL) doesn't work — NULL = NULL is unknown).
    FILTER(ARRAY(
      CASE WHEN age_years < 21 OR age_years > 60 THEN 'AGE_OUT_OF_RANGE' END,
      CASE WHEN dbr_pct > 50 THEN 'HIGH_DBR' END,
      CASE WHEN is_blacklisted THEN 'BLACKLIST_MATCH' END,
      CASE WHEN loan_to_income_months > 24 THEN 'LOAN_TO_INCOME_HIGH' END,
      CASE WHEN employment_stability = 'informal' THEN 'INFORMAL_EMPLOYMENT' END,
      CASE WHEN employment_stability = 'variable' THEN 'VARIABLE_INCOME' END,
      CASE WHEN sentiment_aggregate = 'has_negative' THEN 'NEGATIVE_CALL_SENTIMENT' END,
      CASE WHEN LOWER(purpose_category) RLIKE '(debt|konsolidasi|refinance|cicilan|utang)'
           THEN 'REFINANCE_PURPOSE' END,
      CASE WHEN LOWER(purpose_urgency) RLIKE '(urgent|urgen|mendesak|cepat)'
           THEN 'URGENT_REQUEST' END
    ), x -> x IS NOT NULL) AS decision_reason_codes,
    -- Score
    100
      - CASE WHEN dbr_pct > 50 THEN 40 ELSE 0 END
      - CASE WHEN age_years < 21 OR age_years > 60 THEN 60 ELSE 0 END
      - CASE WHEN is_blacklisted THEN 100 ELSE 0 END
      - CASE WHEN loan_to_income_months > 24 THEN 15 ELSE 0 END
      - CASE WHEN employment_stability = 'informal' THEN 15 ELSE 0 END
      - CASE WHEN employment_stability = 'variable' THEN 8 ELSE 0 END
      - CASE WHEN sentiment_aggregate = 'has_negative' THEN 20 ELSE 0 END
      - CASE WHEN LOWER(purpose_category) RLIKE '(debt|konsolidasi|refinance|cicilan|utang)' THEN 5 ELSE 0 END
      - CASE WHEN LOWER(purpose_urgency)  RLIKE '(urgent|urgen|mendesak|cepat)' THEN 5 ELSE 0 END
      AS risk_score_raw
  FROM base
)
SELECT
  application_id, applicant_name, age_years, gender,
  kecamatan, kabupaten, provinsi, application_ts, channel,
  requested_amount_idr, tenor_months,
  extracted_income_idr, monthly_existing_debt_idr,
  dbr_pct, loan_to_income_months,
  occupation_category, employer_sector, employment_type, employment_stability,
  purpose_category, purpose_urgency,
  total_calls, negative_calls, sentiment_aggregate,
  decision_reason_codes,
  GREATEST(0, LEAST(100, risk_score_raw)) AS risk_score,
  CASE
    WHEN is_blacklisted
      OR age_years < 21 OR age_years > 60
      OR dbr_pct > 50                       THEN 'REJECT'
    WHEN risk_score_raw >= 70               THEN 'APPROVE'
    WHEN risk_score_raw >= 40               THEN 'REVIEW'
    ELSE                                         'REJECT'
  END AS decision
FROM scored
""")

print(f"Built {CATALOG}.gold.vw_application_decisioning")

# COMMAND ----------

display(spark.sql(f"""
SELECT decision, COUNT(*) AS n,
       ROUND(AVG(risk_score), 1) AS avg_score,
       ROUND(AVG(requested_amount_idr), 0) AS avg_loan_idr
FROM {CATALOG}.gold.vw_application_decisioning
GROUP BY decision
ORDER BY decision
"""))

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn — find the "stable but rejected" cases
# MAGIC
# MAGIC The whole point of stacking AI signals on top of hard rules is to catch *applications that look fine on paper but fail under deeper scrutiny*. Write a query that lists applicants who:
# MAGIC
# MAGIC - have `employment_stability = stable`, AND
# MAGIC - ended up with `decision = REJECT` or `REVIEW`.
# MAGIC
# MAGIC Order by descending `risk_score` so the borderline cases come first.

# COMMAND ----------

# YOUR TURN
# display(spark.sql(f"""
# SELECT ...
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT application_id, applicant_name, kabupaten,
       requested_amount_idr, dbr_pct,
       employment_stability, sentiment_aggregate, purpose_category,
       decision, risk_score, decision_reason_codes
FROM {CATALOG}.gold.vw_application_decisioning
WHERE employment_stability = 'stable'
  AND decision IN ('REJECT','REVIEW')
ORDER BY risk_score DESC, application_id
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #2 — turn the rule set into a histogram per province
# MAGIC
# MAGIC Build a SQL query that pivots `decision` over `provinsi`. Output one row per province with three columns: `n_approve`, `n_review`, `n_reject`. Sort by `n_reject` descending.

# COMMAND ----------

# YOUR TURN


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT provinsi,
       SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS n_approve,
       SUM(CASE WHEN decision = 'REVIEW'  THEN 1 ELSE 0 END) AS n_review,
       SUM(CASE WHEN decision = 'REJECT'  THEN 1 ELSE 0 END) AS n_reject,
       COUNT(*) AS total
FROM {CATALOG}.gold.vw_application_decisioning
GROUP BY provinsi
ORDER BY n_reject DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Notebook complete
# MAGIC
# MAGIC One row per applicant with a full audit trail of why each decision was made. Genie reads this view next.
# MAGIC
# MAGIC **Next:** `05_applicant_360_features.py` — foundational data engineering: window functions, percentiles, and the applicant 360 view.
