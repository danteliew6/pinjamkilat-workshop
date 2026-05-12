# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Risk signals with `ai_classify`, `ai_analyze_sentiment`, and `ai_query`
# MAGIC
# MAGIC **30 minutes.** We stack three AI-Function calls to build a risk-signal layer:
# MAGIC
# MAGIC 1. `ai_classify` — bucket employment stability into `stable` / `variable` / `informal`.
# MAGIC 2. `ai_analyze_sentiment` — score every call-center note as positive / neutral / negative.
# MAGIC 3. `ai_query` — write a one-sentence risk narrative in Bahasa per applicant, combining the above.
# MAGIC
# MAGIC By the end you'll have **`workshop.silver.application_risk_signals`** — one row per applicant with structured risk signals plus a Bahasa narrative.

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

# MAGIC %md ## 🙋 Your Turn #1 — find the suspicious applicants
# MAGIC
# MAGIC Write a SQL query that returns applications where:
# MAGIC - `employment_stability` = `stable`, AND
# MAGIC - `sentiment_aggregate` = `has_negative`
# MAGIC
# MAGIC These are the *interesting* cases: stable on paper but flagged by the call. The Risk Officer wants to triage these first.

# COMMAND ----------

# YOUR TURN — write the query below
# display(spark.sql(f"""
# SELECT ...
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
SELECT
  a.application_id,
  a.applicant_name,
  a.kabupaten,
  a.extracted_occupation_category,
  a.extracted_employer_sector,
  e.employment_stability,
  s.negative_calls,
  s.total_calls,
  SUBSTRING(a.occupation_freetext, 1, 60) AS occupation_preview
FROM {CATALOG}.silver.application_kyc a
JOIN {CATALOG}.silver.application_employment e USING (application_id)
JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
WHERE e.employment_stability = 'stable'
  AND s.sentiment_aggregate = 'has_negative'
ORDER BY s.negative_calls DESC, a.application_id
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## Signal #3 — Risk narrative via `ai_query`
# MAGIC
# MAGIC `ai_query(endpoint_name, prompt)` lets us call any foundation model with any prompt. We use it to write a one-sentence Bahasa narrative per applicant — what a human credit analyst's "first read" might say.
# MAGIC
# MAGIC We do this on the **interesting** subset (stable + negative call), not all 500 rows, to keep the workshop snappy.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.application_narrative AS
WITH suspicious AS (
  SELECT
    a.application_id,
    a.applicant_name,
    a.kabupaten,
    a.occupation_freetext,
    a.requested_amount_idr,
    e.employment_stability,
    s.negative_calls,
    s.total_calls
  FROM {CATALOG}.silver.application_kyc a
  JOIN {CATALOG}.silver.application_employment e USING (application_id)
  JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
  WHERE e.employment_stability = 'stable'
    AND s.sentiment_aggregate = 'has_negative'
)
SELECT
  application_id,
  applicant_name,
  ai_query(
    'databricks-claude-sonnet-4-5',
    CONCAT(
      'Anda adalah analis kredit Bank Demo Sejahtera. Tulis SATU kalimat dalam Bahasa Indonesia ',
      'yang menyoroti risiko utama aplikasi berikut. JANGAN beri keputusan, hanya highlight risiko.\\n\\n',
      'Nama: ', applicant_name, '\\n',
      'Lokasi: ', kabupaten, '\\n',
      'Pekerjaan (apa yang ditulis pelamar): ', occupation_freetext, '\\n',
      'Skor stabilitas pekerjaan (model): ', employment_stability, '\\n',
      'Catatan call center: dari ', CAST(total_calls AS STRING), ' panggilan, ',
                                    CAST(negative_calls AS STRING), ' bernuansa negatif.\\n',
      'Jumlah pinjaman diminta: Rp ', CAST(requested_amount_idr AS STRING), '\\n\\n',
      'Output: satu kalimat narasi risiko.'
    )
  ) AS risk_narrative
FROM suspicious
""")

display(spark.sql(f"""
SELECT application_id, applicant_name, risk_narrative
FROM {CATALOG}.silver.application_narrative
ORDER BY application_id
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn #2 — tune the narrative
# MAGIC
# MAGIC Try changing the prompt to also call out: which of the three signals (stable label, call sentiment, requested amount) the analyst should *verify next*. Run on the first 3 rows only to keep iteration fast.

# COMMAND ----------

# YOUR TURN — tweak the prompt and run on the first 3 rows
# display(spark.sql(f"""
# WITH top3 AS (
#   SELECT application_id, applicant_name, occupation_freetext, employment_stability,
#          negative_calls, total_calls, requested_amount_idr
#   FROM {CATALOG}.silver.application_narrative a
#   JOIN {CATALOG}.silver.application_employment USING (application_id)
#   JOIN {CATALOG}.silver.v_application_call_sentiment USING (application_id)
#   JOIN {CATALOG}.silver.application_kyc USING (application_id)
#   LIMIT 3
# )
# SELECT application_id,
#        ai_query(
#          'databricks-claude-sonnet-4-5',
#          ...   -- tweak the prompt here
#        ) AS narrative
# FROM top3
# """))


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

display(spark.sql(f"""
WITH top3 AS (
  SELECT a.application_id, a.applicant_name, a.occupation_freetext, e.employment_stability,
         s.negative_calls, s.total_calls, a.requested_amount_idr
  FROM {CATALOG}.silver.application_kyc a
  JOIN {CATALOG}.silver.application_employment e USING (application_id)
  JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
  WHERE e.employment_stability = 'stable' AND s.sentiment_aggregate = 'has_negative'
  ORDER BY a.application_id
  LIMIT 3
)
SELECT application_id, applicant_name,
       ai_query(
         'databricks-claude-sonnet-4-5',
         CONCAT(
           'Sebagai analis kredit, beri output 2 kalimat dalam Bahasa Indonesia:\\n',
           '1) Highlight risiko utama.\\n',
           '2) Sebut SATU dari tiga signal yang paling perlu diverifikasi ulang: ',
              'employment_stability, call_sentiment, atau requested_amount.\\n\\n',
           'Nama: ', applicant_name, '\\n',
           'Pekerjaan: ', occupation_freetext, '\\n',
           'Employment stability: ', employment_stability, '\\n',
           'Calls: ', CAST(negative_calls AS STRING), '/', CAST(total_calls AS STRING), ' negatif.\\n',
           'Loan: Rp ', CAST(requested_amount_idr AS STRING)
         )
       ) AS narrative
FROM top3
"""))

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
  COALESCE(s.sentiment_aggregate, 'no_calls') AS sentiment_aggregate,
  n.risk_narrative
FROM {CATALOG}.silver.v_application_kyc_clean a
LEFT JOIN {CATALOG}.silver.application_employment      e USING (application_id)
LEFT JOIN {CATALOG}.silver.v_application_call_sentiment s USING (application_id)
LEFT JOIN {CATALOG}.silver.application_narrative       n USING (application_id)
""")

print(f"Built view {CATALOG}.silver.v_application_risk_signals")
spark.sql(f"SELECT COUNT(*) AS rows FROM {CATALOG}.silver.v_application_risk_signals").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Notebook complete
# MAGIC
# MAGIC Three AI Functions stacked:
# MAGIC | Signal | Function | Table |
# MAGIC |---|---|---|
# MAGIC | Employment stability | `ai_classify` | `silver.application_employment` |
# MAGIC | Call sentiment | `ai_analyze_sentiment` | `silver.call_note_sentiment` (+ aggregation view) |
# MAGIC | Bahasa risk narrative | `ai_query` | `silver.application_narrative` (suspicious subset only) |
# MAGIC
# MAGIC All joined into **`workshop.silver.v_application_risk_signals`**.
# MAGIC
# MAGIC **Next:** `03_silver_gold_decisioning.py` — turn signals into a decision.
