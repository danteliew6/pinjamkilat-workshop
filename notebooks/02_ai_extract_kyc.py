# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — KYC with `ai_extract`
# MAGIC
# MAGIC **30 minutes.** We turn messy Bahasa free-text into structured KYC fields without writing a single regex.
# MAGIC
# MAGIC ## What `ai_extract` does
# MAGIC
# MAGIC `ai_extract(text, ARRAY('field1','field2',...))` runs a Databricks-hosted foundation model on each row and returns a JSON object with the fields you asked for. It works on any language the underlying model supports — Bahasa Indonesia included.
# MAGIC
# MAGIC By the end of this notebook you'll have:
# MAGIC
# MAGIC - **`workshop.silver.application_kyc`** — structured KYC fields per applicant (NIK, DOB, gender from KTP; occupation, sector, income, tenure from application form; purpose category from loan-purpose free-text)

# COMMAND ----------

CATALOG = "workshop"
SAMPLE_SIZE = 500   # keep AI-Function calls fast for the workshop; bronze has ~5,000 rows

# COMMAND ----------

# MAGIC %md ## A first look at the messy text we're dealing with

# COMMAND ----------

display(spark.sql(f"""
SELECT application_id,
       ktp_ocr_text,
       occupation_freetext,
       purpose_freetext
FROM {CATALOG}.bronze.loan_application
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice: the KTP OCR text has inconsistent spacing, multiple formats, sometimes garbled headers. The occupation text is conversational Bahasa with mixed numeric formats (`6 juta`, `4-10jt`, `Rp 6000000`). A regex-heavy ingest would need 50–100 patterns to cover this. `ai_extract` covers it in one SQL call.

# COMMAND ----------

# MAGIC %md ## Hero #1 — Extract KTP fields
# MAGIC
# MAGIC We ask for 5 fields. The model returns a JSON struct; we project each field to its own column.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.application_kyc AS
WITH sample AS (
  SELECT * FROM {CATALOG}.bronze.loan_application
  ORDER BY application_id   -- deterministic ordering before sampling
  LIMIT {SAMPLE_SIZE}
),
ktp_extracted AS (
  SELECT
    application_id,
    applicant_name,
    gender             AS gender_from_form,
    dob                AS dob_from_form,
    nik                AS nik_from_form,
    ktp_ocr_text,
    occupation_freetext,
    purpose_freetext,
    requested_amount_idr,
    tenor_months,
    monthly_existing_debt_idr,
    channel,
    kecamatan, kabupaten, provinsi,
    application_ts,
    -- AI Function call
    ai_extract(
      ktp_ocr_text,
      ARRAY('nik','full_name','gender','date_of_birth','place_of_birth')
    ) AS ktp
  FROM sample
)
SELECT
  application_id, applicant_name, gender_from_form, dob_from_form, nik_from_form,
  occupation_freetext, purpose_freetext,
  requested_amount_idr, tenor_months, monthly_existing_debt_idr, channel,
  kecamatan, kabupaten, provinsi, application_ts,
  ktp.nik              AS ktp_nik,
  ktp.full_name        AS ktp_full_name,
  ktp.gender           AS ktp_gender,
  ktp.date_of_birth    AS ktp_dob,
  ktp.place_of_birth   AS ktp_place_of_birth
FROM ktp_extracted
""")

print(f"Built {CATALOG}.silver.application_kyc")
spark.sql(f"SELECT COUNT(*) AS rows FROM {CATALOG}.silver.application_kyc").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Peek at the extraction quality

# COMMAND ----------

display(spark.sql(f"""
SELECT application_id,
       SUBSTRING(occupation_freetext, 1, 60) AS ktp_input_preview,
       ktp_nik, ktp_full_name, ktp_gender, ktp_dob, ktp_place_of_birth,
       -- compare to what was on the application form
       nik_from_form, gender_from_form, dob_from_form
FROM {CATALOG}.silver.application_kyc
ORDER BY application_id
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Mismatches between `ktp_*` (what the model read off the KTP image) and `*_from_form` (what the applicant typed) are real-life KYC findings — fraud detection often starts here.

# COMMAND ----------

# MAGIC %md ## Hero #2 — Extract occupation/income from Bahasa free-text

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.application_kyc AS
SELECT
  k.*,
  occ.occupation_category   AS extracted_occupation_category,
  occ.employer_sector       AS extracted_employer_sector,
  occ.income_idr            AS extracted_income_text,
  occ.tenure_years          AS extracted_tenure_text,
  occ.employment_type       AS extracted_employment_type
FROM (
  SELECT *,
    ai_extract(
      occupation_freetext,
      ARRAY('occupation_category','employer_sector','income_idr','tenure_years','employment_type')
    ) AS occ
  FROM {CATALOG}.silver.application_kyc
) k
""")

print("Occupation extraction added")

# COMMAND ----------

display(spark.sql(f"""
SELECT application_id,
       SUBSTRING(occupation_freetext, 1, 70) AS occupation_input,
       extracted_occupation_category, extracted_employer_sector,
       extracted_income_text, extracted_tenure_text, extracted_employment_type
FROM {CATALOG}.silver.application_kyc
ORDER BY application_id
LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🙋 Your Turn — extract purpose category
# MAGIC
# MAGIC The `purpose_freetext` column has a Bahasa one-sentence description of why the applicant wants the loan. We'd like three structured fields out of it:
# MAGIC
# MAGIC - **`purpose_category`** — one of: `renovation`, `business`, `education`, `debt_consolidation`, `medical`, `vehicle`, `housing_dp`, `wedding`, `other`
# MAGIC - **`urgency`** — one of: `urgent`, `normal`
# MAGIC - **`mentioned_amount_idr`** — any IDR figure mentioned in the text, or null
# MAGIC
# MAGIC Fill in the `ai_extract` call below. It should add three new columns to `silver.application_kyc`.

# COMMAND ----------

# YOUR TURN — fill in the ai_extract call
#
# spark.sql(f"""
# CREATE OR REPLACE TABLE {CATALOG}.silver.application_kyc AS
# SELECT
#   k.*,
#   purp.purpose_category       AS extracted_purpose_category,
#   purp.urgency                AS extracted_purpose_urgency,
#   purp.mentioned_amount_idr   AS extracted_purpose_amount_text
# FROM (
#   SELECT *,
#     ai_extract(
#       purpose_freetext,
#       ARRAY(  -- <-- fill in the field names
#       )
#     ) AS purp
#   FROM {CATALOG}.silver.application_kyc
# ) k
# """)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Solution

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.application_kyc AS
SELECT
  k.*,
  purp.purpose_category       AS extracted_purpose_category,
  purp.urgency                AS extracted_purpose_urgency,
  purp.mentioned_amount_idr   AS extracted_purpose_amount_text
FROM (
  SELECT *,
    ai_extract(
      purpose_freetext,
      ARRAY('purpose_category','urgency','mentioned_amount_idr')
    ) AS purp
  FROM {CATALOG}.silver.application_kyc
) k
""")

display(spark.sql(f"""
SELECT application_id, SUBSTRING(purpose_freetext, 1, 80) AS purpose_input,
       extracted_purpose_category, extracted_purpose_urgency, extracted_purpose_amount_text
FROM {CATALOG}.silver.application_kyc
ORDER BY application_id
LIMIT 12
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Presenter step) Normalize `extracted_income_text` to a numeric column
# MAGIC
# MAGIC The model returns income in varied formats: `"6000000"`, `"10jt"`, `"4-10jt"`, `null`. A small SQL `CASE` over `RLIKE` patterns gives us a numeric `extracted_income_idr`. We'll *use* this column in later notebooks — no exercise here, this is plumbing.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.v_application_kyc_clean AS
SELECT *,
  CASE
    WHEN extracted_income_text RLIKE '^[0-9]+$'
      THEN CAST(extracted_income_text AS BIGINT)
    WHEN extracted_income_text RLIKE '^[0-9]+\\\\.[0-9]+$'
      THEN CAST(CAST(extracted_income_text AS DOUBLE) AS BIGINT)
    WHEN extracted_income_text RLIKE '^[0-9]+(\\\\s*)(jt|juta)$'
      THEN CAST(REGEXP_EXTRACT(extracted_income_text, '^([0-9]+)', 1) AS BIGINT) * 1000000
    WHEN extracted_income_text RLIKE '^[0-9]+(\\\\s*)-(\\\\s*)[0-9]+(\\\\s*)(jt|juta)$'
      THEN CAST(
        (CAST(REGEXP_EXTRACT(extracted_income_text, '^([0-9]+)', 1) AS BIGINT)
         + CAST(REGEXP_EXTRACT(extracted_income_text, '-(\\\\s*)([0-9]+)', 2) AS BIGINT))
         * 500000  -- midpoint × 1,000,000 / 2
      AS BIGINT)
    ELSE NULL
  END AS extracted_income_idr
FROM {CATALOG}.silver.application_kyc
""")

display(spark.sql(f"""
SELECT application_id, extracted_income_text, extracted_income_idr
FROM {CATALOG}.silver.v_application_kyc_clean
WHERE extracted_income_text IS NOT NULL
ORDER BY application_id
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🙋 Your Turn #2 — `ai_classify` vs `ai_extract`
# MAGIC
# MAGIC Look at `extracted_purpose_category` above — the values are all over the place: `"transportation"`, `"Biaya pernikahan"`, `"business"`, `"Konsolidasi utang kartu kredit"`. Useful, but **hard to aggregate** in a dashboard.
# MAGIC
# MAGIC `ai_classify(text, ARRAY(labels))` solves this: it forces the model to pick **one** label from a fixed list. Add a strict-label column to compare.
# MAGIC
# MAGIC **Your task:** fill in one `ai_classify` call. Allowed labels: `renovation`, `business`, `medical`, `education`, `debt_consolidation`, `vehicle`, `wedding`, `other`.

# COMMAND ----------

# YOUR TURN — fill in the ai_classify call.
#
# spark.sql(f"""
# CREATE OR REPLACE VIEW {CATALOG}.silver.v_purpose_compared AS
# SELECT application_id,
#        purpose_freetext,
#        extracted_purpose_category                AS freeform_label,
#        ai_classify(  -- <-- fill this in
#        )                                          AS strict_label
# FROM {CATALOG}.silver.application_kyc
# """)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Solution

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.silver.v_purpose_compared AS
SELECT application_id,
       purpose_freetext,
       extracted_purpose_category                AS freeform_label,
       ai_classify(
         purpose_freetext,
         ARRAY('renovation','business','medical','education',
               'debt_consolidation','vehicle','wedding','other')
       )                                          AS strict_label
FROM {CATALOG}.silver.application_kyc
""")

# Now compare. The free-form column has dozens of unique values; the strict
# column has at most 8. The "Aha" is in this row count diff.
display(spark.sql(f"""
SELECT
  (SELECT COUNT(DISTINCT freeform_label) FROM {CATALOG}.silver.v_purpose_compared) AS freeform_unique_labels,
  (SELECT COUNT(DISTINCT strict_label)   FROM {CATALOG}.silver.v_purpose_compared) AS strict_unique_labels
"""))

display(spark.sql(f"""
SELECT strict_label, COUNT(*) AS n
FROM {CATALOG}.silver.v_purpose_compared
GROUP BY strict_label
ORDER BY n DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeaway:** `ai_extract` is freeform — great for *unknown* fields the model needs to discover. `ai_classify` is constrained — great when you already know the bucket list and need clean aggregation. Use them together.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Notebook complete
# MAGIC
# MAGIC You now have **`workshop.silver.application_kyc`** with structured KYC + occupation + purpose fields, **`v_application_kyc_clean`** with numeric `extracted_income_idr`, and **`v_purpose_compared`** showing the free-form-vs-constrained label trade-off.
# MAGIC
# MAGIC **Next:** `03_ai_classify_risk.py` — `ai_classify` employment stability + `ai_analyze_sentiment` on call notes.
