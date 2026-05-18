# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 — AI/BI Dashboard + Genie space
# MAGIC
# MAGIC **35 minutes.** Put a face on the gold view:
# MAGIC
# MAGIC 1. A small AI/BI Dashboard with KPI tiles, a denial-by-reason bar, and a decision-by-province pivot.
# MAGIC 2. A **Genie space** over the gold view. Risk officers ask questions in Bahasa or English; Genie generates SQL.
# MAGIC
# MAGIC We build both programmatically via the Databricks REST API so the exercise is reproducible.

# COMMAND ----------

CATALOG = "workshop"

# Use the Databricks SDK to talk to the workspace. On a notebook, the SDK
# auto-discovers host + auth from the runtime environment.
import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")

current_user = spark.sql("SELECT current_user()").collect()[0][0]
WORKSPACE_PATH = f"/Users/{current_user}"

# Discover a serverless SQL warehouse. Free Edition auto-creates one.
warehouses = list(w.warehouses.list())
serverless = [wh for wh in warehouses if getattr(wh, "enable_serverless_compute", False)]
if not serverless:
    serverless = warehouses
WAREHOUSE_ID = serverless[0].id

print("Host        :", HOST)
print("User path   :", WORKSPACE_PATH)
print("Warehouse id:", WAREHOUSE_ID, "(", serverless[0].name, ")")

# COMMAND ----------

# MAGIC %md ## A) Sanity-check the gold view

# COMMAND ----------

display(spark.sql(f"""
SELECT
  COUNT(*) AS total_apps,
  SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS approve_n,
  SUM(CASE WHEN decision = 'REVIEW'  THEN 1 ELSE 0 END) AS review_n,
  SUM(CASE WHEN decision = 'REJECT'  THEN 1 ELSE 0 END) AS reject_n,
  ROUND(AVG(risk_score), 1) AS avg_risk_score
FROM {CATALOG}.gold.vw_application_decisioning
"""))

# COMMAND ----------

# MAGIC %md ## B) Create a Genie space programmatically
# MAGIC
# MAGIC Genie spaces are created via `POST /api/2.0/genie/spaces`. The body needs a `serialized_space` blob with table descriptions, sample questions, and (optionally) general instructions. The skill ships with column-level comments on the gold view, so Genie picks those up automatically.

# COMMAND ----------

import uuid
# HOST, TOKEN, HEADERS were initialized in the warehouse-discovery cell above.

# COMMAND ----------

SAMPLE_QUESTIONS = [
    # Decisioning-view questions
    "Show me applications rejected in the last 14 days, broken down by reason code.",
    "Aplikasi mana yang ter-flag karena pendapatan tidak konsisten minggu ini?",
    "Which kabupaten has the highest reject rate?",
    "Top 5 occupation categories by approved loan volume in IDR.",
    "Show me REJECT applications where employment_stability is 'stable'.",
    "Average DBR by employment_type, only APPROVE applications.",
    "Distribution of decision by channel (APP / AGENT / BRANCH).",
    "Which applicants have a risk_score below 30 and what's in their decision_reason_codes?",
    # Applicant-360 questions (new behavioural + financial view)
    "Show applicants who declared income more than 50% higher than observed bank salary.",
    "Which approved applicants have negative savings velocity? (spending more than earning)",
    "Top 10 applicants by salary_consistency_score (highest = most volatile income).",
    "Average expense_to_income_ratio by employment_stability — APPROVE only.",
    "Berapa banyak pemohon dengan declared_vs_observed_pct di bawah 80? (kemungkinan under-disclose)",
]

INSTRUCTIONS = """\
You are a credit-risk analytics assistant for Bank Demo Sejahtera, the fictional Indonesian
neobank operating the PinjamKilat digital-loan product.

Rules:
- Currency is Indonesian Rupiah (IDR). Never convert to USD. Format large numbers with thousand separators.
- decision is one of: APPROVE, REVIEW, REJECT.
- decision_reason_codes is an ARRAY. Use ARRAY_CONTAINS or EXPLODE when filtering on a specific code.
- employment_stability is one of: stable, variable, informal.
- sentiment_aggregate is one of: has_negative, positive_only, neutral_only, no_calls.
- DBR > 50% is an automatic reject. Loan-to-income > 24 months is a review trigger.
- When the user says "minggu ini" / "this week", default to last 7 days based on application_ts.
- When the user says "bulan ini" / "this month", use DATE_TRUNC('MONTH', application_ts).
"""

def get_column_descriptions(table_fqn: str):
    """Pull column comments straight from the gold view DDL."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table_fqn}").collect()
    cols = []
    for r in rows:
        if r["col_name"].startswith("#") or r["col_name"] == "":
            break
        cols.append({"column_name": r["col_name"],
                     "description": [r["comment"] or ""]})
    return sorted(cols, key=lambda c: c["column_name"])

# Certified Q->SQL pairs ("trusted examples"). Genie uses these as patterns
# when answering similar questions. Each pair targets a different join shape
# or aggregation style so Genie generalises.
CERTIFIED_QUERIES = [
    {
        "question": "Top 5 kabupaten by reject rate (last 90 days).",
        "sql": f"""
WITH last90 AS (
  SELECT kabupaten, decision
  FROM {CATALOG}.gold.vw_application_decisioning
  WHERE application_ts >= date_sub(current_date(), 90)
)
SELECT kabupaten,
       COUNT(*) AS total_apps,
       SUM(CASE WHEN decision = 'REJECT' THEN 1 ELSE 0 END) AS rejects,
       ROUND(100.0 * SUM(CASE WHEN decision = 'REJECT' THEN 1 ELSE 0 END) / COUNT(*), 2) AS reject_pct
FROM last90
GROUP BY kabupaten
HAVING COUNT(*) >= 5
ORDER BY reject_pct DESC
LIMIT 5""".strip(),
    },
    {
        "question": "Which applicants declared income materially higher than observed bank-statement salary?",
        "sql": f"""
SELECT application_id, applicant_name, kabupaten,
       declared_income_idr, observed_avg_salary_idr, declared_vs_observed_pct,
       decision
FROM {CATALOG}.gold.vw_applicant_360
WHERE declared_vs_observed_pct > 150
  AND observed_avg_salary_idr > 0
ORDER BY declared_vs_observed_pct DESC
LIMIT 25""".strip(),
    },
    {
        "question": "Approval rate by employment_stability bucket.",
        "sql": f"""
SELECT employment_stability,
       COUNT(*) AS n_apps,
       SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS n_approved,
       ROUND(100.0 * SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) / COUNT(*), 2) AS approve_pct
FROM {CATALOG}.gold.vw_application_decisioning
WHERE employment_stability IS NOT NULL
GROUP BY employment_stability
ORDER BY approve_pct DESC""".strip(),
    },
    {
        "question": "Top reason codes triggering review (last 30 days).",
        "sql": f"""
WITH recent AS (
  SELECT EXPLODE(decision_reason_codes) AS reason
  FROM {CATALOG}.gold.vw_application_decisioning
  WHERE decision = 'REVIEW'
    AND application_ts >= date_sub(current_date(), 30)
)
SELECT reason, COUNT(*) AS n
FROM recent
GROUP BY reason
ORDER BY n DESC""".strip(),
    },
    {
        "question": "Applicants spending more than they earn (expense_to_income_ratio > 1) — by decision.",
        "sql": f"""
SELECT decision,
       COUNT(*) AS n_apps,
       ROUND(AVG(expense_to_income_ratio), 2) AS avg_ratio,
       ROUND(AVG(savings_velocity_idr), 0) AS avg_savings_velocity_idr
FROM {CATALOG}.gold.vw_applicant_360
WHERE expense_to_income_ratio > 1
GROUP BY decision
ORDER BY n_apps DESC""".strip(),
    },
    {
        "question": "Average requested loan amount by channel, only APPROVE.",
        "sql": f"""
SELECT channel,
       COUNT(*) AS n_approved,
       ROUND(AVG(requested_amount_idr), 0) AS avg_loan_idr,
       ROUND(AVG(tenor_months), 1) AS avg_tenor_months
FROM {CATALOG}.gold.vw_application_decisioning
WHERE decision = 'APPROVE'
GROUP BY channel
ORDER BY avg_loan_idr DESC""".strip(),
    },
]

# Benchmark questions — used to test Genie's accuracy. Each question has a
# reference SQL answer. We run them after creating the space and compare.
BENCHMARK_QUESTIONS = [
    {
        "question": "How many applications were rejected this month?",
        "sql": f"""
SELECT COUNT(*) AS n_rejected
FROM {CATALOG}.gold.vw_application_decisioning
WHERE decision = 'REJECT'
  AND application_ts >= DATE_TRUNC('MONTH', current_date())""".strip(),
    },
    {
        "question": "What is the overall approval rate across all applications?",
        "sql": f"""
SELECT ROUND(100.0 * SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) / COUNT(*), 2) AS approve_pct
FROM {CATALOG}.gold.vw_application_decisioning""".strip(),
    },
    {
        "question": "Which province has the most applications?",
        "sql": f"""
SELECT provinsi, COUNT(*) AS n
FROM {CATALOG}.gold.vw_application_decisioning
GROUP BY provinsi
ORDER BY n DESC
LIMIT 1""".strip(),
    },
    {
        "question": "Show the top 3 applicants by salary_consistency_score (worst = highest score).",
        "sql": f"""
SELECT applicant_name, employment_stability, salary_consistency_score
FROM {CATALOG}.gold.vw_applicant_360
WHERE salary_consistency_score IS NOT NULL
ORDER BY salary_consistency_score DESC
LIMIT 3""".strip(),
    },
    {
        "question": "Count applications with HIGH_DBR in their decision_reason_codes.",
        "sql": f"""
SELECT COUNT(*) AS n
FROM {CATALOG}.gold.vw_application_decisioning
WHERE ARRAY_CONTAINS(decision_reason_codes, 'HIGH_DBR')""".strip(),
    },
]

def sql_to_list(s: str):
    """Convert a multi-line SQL string into the list-of-lines format Genie expects."""
    return [line + "\n" for line in s.split("\n")]


serialized = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLE_QUESTIONS
        ]
    },
    "data_sources": {
        "tables": [
            # Both gold views — kept sorted by identifier (Genie API requirement).
            {
                "identifier": f"{CATALOG}.gold.vw_applicant_360",
                "description": [
                    "ONE ROW PER APPLICANT. Behavioural / financial view joining "
                    "decisioning, 12-month bank-transaction features, and call summary. "
                    "Use this for questions about declared-vs-observed income, salary "
                    "consistency, expense-to-income ratio, savings velocity, and "
                    "transaction-derived risk signals."
                ],
                "column_configs": get_column_descriptions(f"{CATALOG}.gold.vw_applicant_360"),
            },
            {
                "identifier": f"{CATALOG}.gold.vw_application_decisioning",
                "description": [
                    "ONE ROW PER APPLICATION. Combines AI-Function-derived signals "
                    "(employment stability, call sentiment, KYC extraction) with hard "
                    "banking rules (DBR, age, blacklist) into one decision per applicant. "
                    "Primary view for raw decisioning / rules questions."
                ],
                "column_configs": get_column_descriptions(f"{CATALOG}.gold.vw_application_decisioning"),
            },
        ]
    },
    "instructions": {
        "text_instructions": [{"id": uuid.uuid4().hex, "content": [INSTRUCTIONS]}],
        # Genie API requires these arrays to be sorted by id.
        "example_question_sqls": sorted([
            {
                "id": uuid.uuid4().hex,
                "question": [cq["question"]],
                "sql": sql_to_list(cq["sql"]),
            }
            for cq in CERTIFIED_QUERIES
        ], key=lambda x: x["id"]),
    },
    "benchmarks": {
        "questions": sorted([
            {
                "id": uuid.uuid4().hex,
                "question": [bq["question"]],
                "answer": [{"format": "SQL", "content": sql_to_list(bq["sql"])}],
            }
            for bq in BENCHMARK_QUESTIONS
        ], key=lambda x: x["id"])
    },
}

# COMMAND ----------

# Idempotent create-or-update via the SDK's raw API client.
list_resp = w.api_client.do("GET", "/api/2.0/genie/spaces") or {}
existing = [s for s in list_resp.get("spaces", []) if s.get("title") == "PinjamKilat Workshop — Credit Risk"]

payload = {
    "title": "PinjamKilat Workshop — Credit Risk",
    "description": "Natural-language credit-risk analytics for Bank Demo Sejahtera's PinjamKilat product (synthetic Indonesian data).",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized),
}

if existing:
    sid = existing[0]["space_id"]
    print(f"Updating existing Genie space {sid}")
    resp = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{sid}", body=payload)
else:
    print("Creating new Genie space")
    resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)

GENIE_SPACE_ID = resp["space_id"]
print(f"\n✓ Genie space ready: {GENIE_SPACE_ID}")
print(f"  URL: {HOST}/genie/rooms/{GENIE_SPACE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try the Genie space now
# MAGIC
# MAGIC Click the URL printed above. Pick any sample question from the pre-warmed list (they appear as suggestion chips). For example:
# MAGIC
# MAGIC > *"Show me applications rejected in the last 14 days, broken down by reason code."*
# MAGIC
# MAGIC Then try one in Bahasa:
# MAGIC
# MAGIC > *"Aplikasi mana yang ter-flag karena pendapatan tidak konsisten minggu ini?"*
# MAGIC
# MAGIC Genie generates SQL using the column descriptions we wrote into the gold view DDL.

# COMMAND ----------

# MAGIC %md ## Helper — `ask_genie()` (we'll use this for both your turn and the benchmark)
# MAGIC
# MAGIC The Genie Conversation API is two HTTP calls: `start-conversation` then poll `messages/{id}` until the status flips to `COMPLETED`. We wrap it once.

# COMMAND ----------

import time

def ask_genie(space_id: str, question: str, timeout_sec: int = 90) -> dict:
    """Ask Genie a question; return the first completed message."""
    body = w.api_client.do("POST",
        f"/api/2.0/genie/spaces/{space_id}/start-conversation",
        body={"content": question})
    cid, mid = body["conversation_id"], body["message_id"]

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        msg = w.api_client.do("GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}")
        if msg.get("status") in ("COMPLETED", "FAILED"):
            return msg
        time.sleep(3)
    raise TimeoutError(f"Genie didn't respond within {timeout_sec}s")

# COMMAND ----------

# MAGIC %md ## 🙋 Your Turn — ask Genie a question of your own
# MAGIC
# MAGIC One line. Pick anything — *"top 5 occupations by approved loan IDR"*, *"average DBR by province"*, anything in Bahasa or English. Print the generated SQL.

# COMMAND ----------

# YOUR TURN — call ask_genie with your question.
# answer = ask_genie(GENIE_SPACE_ID, "<your question here>")
# for a in answer.get("attachments", []):
#     if "query" in a: print("SQL:", a["query"].get("query"))
#     if "text"  in a: print("ANSWER:", a["text"].get("content"))


# COMMAND ----------

# MAGIC %md ### ✅ Solution — one example

# COMMAND ----------

answer = ask_genie(GENIE_SPACE_ID, "Which kabupaten has the highest reject rate? Top 5.")
for a in answer.get("attachments", []):
    if "query" in a:
        print("\nGENERATED SQL:")
        print(a["query"].get("query", "(no sql)"))
    if "text" in a:
        print("\nANSWER:")
        print(a["text"].get("content", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeaway:** Genie is just a REST API behind the UI. Anything that talks HTTP can ask Genie — Streamlit/Dash apps, Slack bots, your team's BI chat layer. The column comments + `example_question_sqls` we configured above are what make the answers grounded.

# COMMAND ----------

# MAGIC %md ## D) Benchmark runner — "is this prototype production-ready?"
# MAGIC
# MAGIC The space now ships with 5 benchmark questions (encoded in `benchmarks.questions`). A real production roll-out would run them on every Genie config change and gate deploys on a passing score.
# MAGIC
# MAGIC Here we:
# MAGIC
# MAGIC 1. Ask each benchmark question via the Conversation API.
# MAGIC 2. Time it.
# MAGIC 3. Capture the SQL Genie generated.
# MAGIC 4. Run our reference SQL.
# MAGIC 5. Compare row counts as a coarse correctness check.
# MAGIC
# MAGIC Caveat: row-count match is a shallow signal. Production grading should diff actual result sets or use an LLM-as-judge. This is the workshop version.

# COMMAND ----------

import time as _time

results = []
for bq in BENCHMARK_QUESTIONS:
    t0 = _time.time()
    msg = ask_genie(GENIE_SPACE_ID, bq["question"], timeout_sec=120)
    elapsed = _time.time() - t0

    genie_sql = None
    for a in msg.get("attachments", []):
        if "query" in a:
            genie_sql = a["query"].get("query")
            break

    # Run the reference SQL ourselves
    ref_rows = spark.sql(bq["sql"]).count()
    # Run Genie's generated SQL (if any) and compare row counts
    genie_rows = None
    sql_ok = False
    if genie_sql:
        try:
            genie_rows = spark.sql(genie_sql).count()
            sql_ok = True
        except Exception as e:
            print(f"  ⚠ Genie SQL did not parse: {e}")

    results.append({
        "question": bq["question"],
        "elapsed_s": round(elapsed, 2),
        "genie_sql_runs": sql_ok,
        "ref_rows": ref_rows,
        "genie_rows": genie_rows,
        "row_count_match": (genie_rows == ref_rows) if sql_ok else None,
    })

import pandas as pd
results_df = pd.DataFrame(results)
display(spark.createDataFrame(results_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the benchmark output
# MAGIC
# MAGIC - `elapsed_s` — wall-clock time per Genie question (includes model + SQL execution).
# MAGIC - `genie_sql_runs` — TRUE if the SQL Genie generated parsed and ran on Spark.
# MAGIC - `row_count_match` — coarse correctness check vs the reference SQL.
# MAGIC
# MAGIC For a real production deployment you'd:
# MAGIC
# MAGIC - Move benchmarks to a separate job that runs on every metadata change.
# MAGIC - Replace row-count match with an actual result-set diff (sorted-set equality).
# MAGIC - Track pass-rate per Genie config version + alert on regression.
# MAGIC - Add latency SLOs (e.g., p95 < 8s for simple aggregate questions).

# COMMAND ----------

# MAGIC %md ## C) Build the AI/BI Dashboard
# MAGIC
# MAGIC We push a minimal dashboard with 4 widgets:
# MAGIC
# MAGIC 1. **Total applications** (KPI counter)
# MAGIC 2. **Reject rate %** (KPI counter)
# MAGIC 3. **Decision by province** (bar chart)
# MAGIC 4. **Top denial reason codes** (table)

# COMMAND ----------

def text_w(name, x, y, w, h, md):
    return {"widget": {"name": name, "textbox_spec": md},
            "position": {"x": x*2, "y": y, "width": w*2, "height": h}}  # 12-col grid

def counter_w(name, x, y, w, h, ds, field, display, fmt=None):
    spec_value = {"fieldName": field, "displayName": display}
    if fmt is not None:
        spec_value["format"] = {"type": fmt}
    return {"widget": {"name": name,
        "queries": [{"name":"main_query","query":{"datasetName":ds,
            "fields":[{"name":field,"expression":f"`{field}`"}],"disaggregated":True}}],
        "spec":{"version":2,"widgetType":"counter",
            "encodings":{"value":spec_value},
            "frame":{"showTitle":True,"title":display}}},
        "position":{"x":x*2,"y":y,"width":w*2,"height":h}}  # 12-col grid

def counter_pct(name, x, y, w, h, ds, field, display):
    return counter_w(name, x, y, w, h, ds, field, display, fmt="number-percent")

# Bar: all aggregation already in dataset SQL (GROUP BY there). Widget uses
# plain `field` refs + disaggregated:true (the working Lakeview convention).
def bar_w(name, x, y, w, h, ds, x_field, y_field, x_disp, y_disp, title,
          color_field=None):
    fields = [{"name":x_field,"expression":f"`{x_field}`"},
              {"name":y_field,"expression":f"`{y_field}`"}]
    enc = {
        "x":{"fieldName":x_field,"displayName":x_disp,"scale":{"type":"categorical"}},
        "y":{"fieldName":y_field,"displayName":y_disp,"scale":{"type":"quantitative"}},
        "label":{"show":True},
    }
    if color_field:
        fields.append({"name":color_field,"expression":f"`{color_field}`"})
        enc["color"] = {"fieldName":color_field,"displayName":color_field,"scale":{"type":"categorical"}}
    return {"widget":{"name":name,
        "queries":[{"name":"main_query","query":{"datasetName":ds,"fields":fields,"disaggregated":True}}],
        "spec":{"version":3,"widgetType":"bar","encodings":enc,
            "frame":{"showTitle":True,"title":title}}},
        "position":{"x":x*2,"y":y,"width":w*2,"height":h}}  # 12-col grid

def table_w(name, x, y, w, h, ds, cols, title=None):
    fields = [{"name": fn, "expression": f"`{fn}`"} for fn, _ in cols]
    enc_cols = [{"fieldName": fn, "displayName": dn} for fn, dn in cols]
    return {"widget":{"name":name,
        "queries":[{"name":"main_query","query":{"datasetName":ds,"fields":fields,"disaggregated":True}}],
        "spec":{"version":2,"widgetType":"table","encodings":{"columns":enc_cols},
            "frame":{"showTitle":True,"title":title or name}}},
        "position":{"x":x*2,"y":y,"width":w*2,"height":h}}  # 12-col grid

dashboard = {
    "datasets": [
        {"name":"ds_total","displayName":"Total apps",
         "queryLines":[f"SELECT COUNT(*) AS total FROM {CATALOG}.gold.vw_application_decisioning"]},
        {"name":"ds_reject_rate","displayName":"Reject rate",
         "queryLines":[f"SELECT ROUND(SUM(CASE WHEN decision='REJECT' THEN 1 ELSE 0 END)*1.0/COUNT(*), 4) AS reject_rate FROM {CATALOG}.gold.vw_application_decisioning"]},
        {"name":"ds_by_province","displayName":"Decisions by province",
         "queryLines":[f"SELECT provinsi, decision, COUNT(*) AS n FROM {CATALOG}.gold.vw_application_decisioning GROUP BY provinsi, decision ORDER BY provinsi"]},
        {"name":"ds_reasons","displayName":"Reason codes",
         "queryLines":[f"WITH x AS (SELECT EXPLODE(decision_reason_codes) AS reason FROM {CATALOG}.gold.vw_application_decisioning WHERE decision IN ('REJECT','REVIEW')) SELECT reason, COUNT(*) AS n FROM x GROUP BY reason ORDER BY n DESC"]},
    ],
    "uiSettings": {
        "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
        "applyModeEnabled": False,
    },
    "pages": [{
        "name": "page1",
        "displayName": "Risk Officer",
        "layoutVersion": "GRID_V1",
        "pageType": "PAGE_TYPE_CANVAS",
        "layout": [
            text_w("title", 0, 0, 6, 1,
                   "# 💳 PinjamKilat — Credit Risk Dashboard\n_Bank Demo Sejahtera · synthetic Indonesian digital-loan data_"),
            counter_w("kpi_total", 0, 1, 2, 3, "ds_total", "total", "Total applications"),
            counter_pct("kpi_reject", 2, 1, 2, 3, "ds_reject_rate", "reject_rate", "Reject rate"),
            text_w("kpi_placeholder", 4, 1, 2, 3,
                   "## Decisions today\n\nUse the breakdowns below + the Genie space for drill-downs."),
            text_w("section_breakdowns", 0, 4, 6, 1, "## Breakdowns"),
            bar_w("by_province", 0, 5, 3, 5, "ds_by_province",
                  "provinsi", "n",
                  "Province", "Applications",
                  "Decisions by province",
                  color_field="decision"),
            table_w("reasons", 3, 5, 3, 5, "ds_reasons",
                    [("reason","Reason code"), ("n","Count")],
                    title="Reason codes"),
        ],
    }],
}

# COMMAND ----------

# Push the dashboard via REST.
dashboard_payload = {
    "display_name": "PinjamKilat — Credit Risk",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_dashboard": json.dumps(dashboard),
    "parent_path": WORKSPACE_PATH,
}

# Try to find an existing dashboard by name, else create. Both use the SDK API client.
list_resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards") or {}
existing_dash = next(
    (d for d in list_resp.get("dashboards", [])
     if d.get("display_name") == "PinjamKilat — Credit Risk"),
    None
)

if existing_dash:
    did = existing_dash["dashboard_id"]
    print(f"Updating dashboard {did}")
    resp = w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{did}", body={
        "display_name": dashboard_payload["display_name"],
        "warehouse_id": dashboard_payload["warehouse_id"],
        "serialized_dashboard": dashboard_payload["serialized_dashboard"],
    })
else:
    print("Creating dashboard")
    resp = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=dashboard_payload)

DASHBOARD_ID = resp["dashboard_id"]
print(f"\n✓ Dashboard ready: {DASHBOARD_ID}")
print(f"  URL: {HOST}/dashboardsv3/{DASHBOARD_ID}")

# COMMAND ----------

# MAGIC %md ## ✅ Workshop complete
# MAGIC
# MAGIC You now have:
# MAGIC
# MAGIC | Asset | Lives at |
# MAGIC |---|---|
# MAGIC | Bronze + silver + gold tables | `workshop.{bronze, silver, gold}.*` |
# MAGIC | Genie space | URL printed above |
# MAGIC | Dashboard | URL printed above |
# MAGIC
# MAGIC Suggested follow-ups:
# MAGIC
# MAGIC - **Schedule** the full 5-notebook job to run daily (use `resources/jobs.yml` in the repo).
# MAGIC - **Add an MLflow-tracked classical model** (LightGBM on the gold view) — see workshop appendix in the repo.
# MAGIC - **Wire a Mosaic AI Vector Search index** over policy PDFs so the Genie space can also answer "what does our SOP say about high-DBR cases?" (requires paid tier).
