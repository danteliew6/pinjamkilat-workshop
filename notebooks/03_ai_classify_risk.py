# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Risk signals with `ai_classify` and `ai_analyze_sentiment`
# MAGIC
# MAGIC **30 minutes.** We stack two AI-Function calls to build a risk-signal layer:
# MAGIC
# MAGIC 1. `ai_classify` — bucket employment stability into `stable` / `variable` / `informal`.
# MAGIC 2. `ai_analyze_sentiment` — score every call-center note as positive / neutral / negative.
# MAGIC
# MAGIC By the end you'll have **`workshop.silver.v_application_risk_signals`** — one row per applicant with structured risk signals ready for the decisioning view.
# MAGIC
# MAGIC > **Note on `ai_query`:** Databricks also offers `ai_query(endpoint_name, prompt)` for generic LLM generation, but it requires a paid foundation-model endpoint. Free Edition workspaces don't have those endpoints exposed, so we skip it here. The patterns transfer directly.

# COMMAND ----------

CATALOG = "workshop"

# COMMAND ----------

# MAGIC %md ## Signal #1 — Employment stability via `ai_classify`
# MAGIC
# MAGIC `ai_classify(text, ARRAY('label1','label2',...))` returns the single best label. Cheap, fast, deterministic.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.application_employment AS
SELECT
  application_id,
  occupation_freetext,
  ai_classify(
    occupation_freetext,
    ARRAY('stable','variable','informal')
  ) AS employment_stability
FROM {CATALOG}.silver.application_kyc
""")

display(spark.sql(f"""
SELECT employment_stability, COUNT(*) AS n
FROM {CATALOG}.silver.application_employment
GROUP BY employment_stability
ORDER BY n DESC
"""))

# COMMAND ----------

# MAGIC %md ## Signal #2 — Call-center sentiment via `ai_analyze_sentiment`
# MAGIC
# MAGIC One applicant can have multiple calls. We sentiment-score every note, then aggregate to one risk signal per applicant: "any negative call" is a flag.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.call_note_sentiment AS
SELECT
  note_id,
  application_id,
  note_text,
  note_ts,
  ai_analyze_sentiment(note_text) AS sentiment
FROM {CATALOG}.bronze.call_note
WHERE application_id IN (SELECT application_id FROM {CATALOG}.silver.application_kyc)
""")

display(spark.sql(f"""
SELECT sentiment, COUNT(*) AS n
FROM {CATALOG}.silver.call_note_sentiment
GROUP BY sentiment
ORDER BY n DESC
"""))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.v_application_call_sentiment AS
SELECT
  application_id,
  COUNT(*)                                                AS total_calls,
  SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_calls,
  SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_calls,
  SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neutral_calls,
  CASE
    WHEN SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) >= 1 THEN 'has_negative'
    WHEN SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) >= 1 THEN 'positive_only'
    ELSE 'neutral_only'
  END AS sentiment_aggregate
FROM {CATALOG}.silver.call_note_sentiment
GROUP BY application_id
""")

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #1 — stack two AI signals with one join
# MAGIC
# MAGIC The whole point of producing structured signals from AI Functions is that they can be **joined and filtered like any other column**. Combine the two we just built:
# MAGIC
# MAGIC - `silver.application_employment` (one row per applicant, one column `employment_stability`)
# MAGIC - `silver.v_application_call_sentiment` (one row per applicant, `negative_calls` etc.)
# MAGIC
# MAGIC **Your task:** return applicants whose `employment_stability = 'stable'` **and** who have at least one negative call. Two-table join, two `WHERE` conditions.

# COMMAND ----------

# YOUR TURN — fill in the SELECT and the JOIN.
# display(spark.sql(f"""
# SELECT e.application_id, e.employment_stability, s.negative_calls
# FROM {CATALOG}.silver.application_employment e
# JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
# WHERE ...
# ORDER BY s.negative_calls DESC
# LIMIT 20
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT e.application_id, e.employment_stability, s.negative_calls, s.total_calls
FROM {CATALOG}.silver.application_employment e
JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
WHERE e.employment_stability = 'stable'
  AND s.negative_calls >= 1
ORDER BY s.negative_calls DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeaway:** AI Functions emit ordinary columns. The risk-officer workflow ("find people who *look* fine but smell off") becomes a 4-line SQL query.

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #2 — `ai_classify` with custom banking labels
# MAGIC
# MAGIC We sentiment-scored the call notes above. Sentiment is generic. For banking, **what kind of risk** is more useful — fraud signal, verification concern, normal? Same input text, different label set.
# MAGIC
# MAGIC **Your task:** classify each call note into one of: `fraud_concern`, `verification_concern`, `positive_interaction`, `other`. One `ai_classify` call.

# COMMAND ----------

# YOUR TURN — fill in the ai_classify call below.
# display(spark.sql(f"""
# SELECT note_id, application_id,
#        SUBSTRING(note_text, 1, 80) AS note_preview,
#        ai_classify(  -- <-- fill this in
#        ) AS call_label
# FROM {CATALOG}.bronze.call_note
# LIMIT 10
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT note_id, application_id,
       SUBSTRING(note_text, 1, 80) AS note_preview,
       ai_classify(
         note_text,
         ARRAY('fraud_concern','verification_concern','positive_interaction','other')
       ) AS call_label
FROM {CATALOG}.bronze.call_note
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeaway:** `ai_classify` lets you re-label the *same input* with *different label sets*. Sentiment is a general signal; banking-specific labels (`fraud_concern`) are actionable. Stack both for richer risk views — and remember: no model training required.

# COMMAND ----------

# MAGIC %md ## Assemble the unified risk-signals view

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.v_application_risk_signals AS
SELECT
  a.application_id,
  a.applicant_name, a.kabupaten, a.provinsi,
  a.requested_amount_idr, a.tenor_months,
  a.monthly_existing_debt_idr,
  a.extracted_income_idr,
  a.extracted_occupation_category, a.extracted_employer_sector,
  a.extracted_employment_type,
  a.extracted_purpose_category, a.extracted_purpose_urgency,
  a.application_ts,
  e.employment_stability,
  COALESCE(s.total_calls, 0)    AS total_calls,
  COALESCE(s.negative_calls, 0) AS negative_calls,
  COALESCE(s.positive_calls, 0) AS positive_calls,
  COALESCE(s.sentiment_aggregate, 'no_calls') AS sentiment_aggregate
FROM {CATALOG}.silver.v_application_kyc_clean a
LEFT JOIN {CATALOG}.silver.application_employment      e USING (application_id)
LEFT JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
""")

print(f"Built view {CATALOG}.silver.v_application_risk_signals")
spark.sql(f"SELECT COUNT(*) AS rows FROM {CATALOG}.silver.v_application_risk_signals").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Notebook complete
# MAGIC
# MAGIC Two AI Functions stacked:
# MAGIC | Signal | Function | Table |
# MAGIC |---|---|---|
# MAGIC | Employment stability | `ai_classify` | `silver.application_employment` |
# MAGIC | Call sentiment | `ai_analyze_sentiment` | `silver.call_note_sentiment` (+ aggregation view) |
# MAGIC
# MAGIC All joined into **`workshop.silver.v_application_risk_signals`**.
# MAGIC
# MAGIC **Next:** `04_silver_gold_decisioning.py` — turn signals into a decision.
