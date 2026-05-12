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
    "Show me applications rejected in the last 14 days, broken down by reason code.",
    "Aplikasi mana yang ter-flag karena pendapatan tidak konsisten minggu ini?",
    "Which kabupaten has the highest reject rate?",
    "Top 5 occupation categories by approved loan volume in IDR.",
    "Show me REJECT applications where employment_stability is 'stable'.",
    "Average DBR by employment_type, only APPROVE applications.",
    "Distribution of decision by channel (APP / AGENT / BRANCH).",
    "Berapa persen aplikasi BPJS — eh, maksudnya aplikasi dengan sentimen call negatif yang akhirnya di-REJECT?",
    "Which applicants have a risk_score below 30 and what's in their decision_reason_codes?",
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

serialized = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLE_QUESTIONS
        ]
    },
    "data_sources": {
        "tables": [
            {
                "identifier": f"{CATALOG}.gold.vw_application_decisioning",
                "description": [
                    "ONE ROW PER APPLICATION. Combines AI-Function-derived signals "
                    "(employment stability, call sentiment, KYC extraction) with hard "
                    "banking rules (DBR, age, blacklist) into one decision per applicant. "
                    "Primary view for risk-officer analytics."
                ],
                "column_configs": get_column_descriptions(f"{CATALOG}.gold.vw_application_decisioning"),
            }
        ]
    },
    "instructions": {
        "text_instructions": [{"id": uuid.uuid4().hex, "content": [INSTRUCTIONS]}]
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

# MAGIC %md ## 🙋 Your Turn — ask a question programmatically
# MAGIC
# MAGIC Use the Genie Conversation API to ask a question and grab Genie's SQL response. This is the same pattern you'd use to embed Genie behind a custom UI or chatbot.

# COMMAND ----------

# YOUR TURN — call /api/2.0/genie/spaces/{sid}/start-conversation
# and poll until message status is COMPLETED.
#
# Hint: the start-conversation endpoint takes {"content": "<question>"} and returns
# {"conversation_id": ..., "message_id": ...}.
# Then GET /api/2.0/genie/spaces/{sid}/conversations/{cid}/messages/{mid}
# and read the 'attachments' field for the generated SQL + answer text.


# COMMAND ----------

# MAGIC %md ### ✅ Solution

# COMMAND ----------

import time

def ask_genie(space_id: str, question: str, timeout_sec: int = 90) -> dict:
    """Ask Genie a question; return the first completed message."""
    body = w.api_client.do("POST",
        f"/api/2.0/genie/spaces/{space_id}/start-conversation",
        body={"content": question})
    cid, mid = body["conversation_id"], body["message_id"]
    print(f"  conversation={cid}  message={mid}")

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        msg = w.api_client.do("GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}")
        status = msg.get("status")
        if status in ("COMPLETED", "FAILED"):
            return msg
        time.sleep(3)
    raise TimeoutError(f"Genie didn't respond within {timeout_sec}s")


answer = ask_genie(GENIE_SPACE_ID, "Which kabupaten has the highest reject rate? Top 5.")

# Pull out the SQL + the natural-language summary
for a in answer.get("attachments", []):
    if "query" in a:
        print("\nGENERATED SQL:")
        print(a["query"].get("query", "(no sql)"))
    if "text" in a:
        print("\nANSWER:")
        print(a["text"].get("content", ""))

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
            "position": {"x": x, "y": y, "width": w, "height": h}}

def counter_w(name, x, y, w, h, ds, field, display, fmt="number-plain"):
    return {"widget": {"name": name,
        "queries": [{"name":"q","query":{"datasetName":ds,
            "fields":[{"name":field,"expression":f"`{field}`"}],"disaggregated":True}}],
        "spec":{"version":2,"widgetType":"counter","encodings":{
            "value":{"fieldName":field,"displayName":display,"format":{"type":fmt}}}}},
        "position":{"x":x,"y":y,"width":w,"height":h}}

def counter_pct(name, x, y, w, h, ds, field, display):
    return {"widget": {"name": name,
        "queries": [{"name":"q","query":{"datasetName":ds,
            "fields":[{"name":field,"expression":f"`{field}`"}],"disaggregated":True}}],
        "spec":{"version":2,"widgetType":"counter","encodings":{
            "value":{"fieldName":field,"displayName":display,
            "format":{"type":"number-percent","decimalPlaces":{"type":"max","places":1}}}}}},
        "position":{"x":x,"y":y,"width":w,"height":h}}

def bar_w(name, x, y, w, h, ds, x_field, y_field, x_expr, y_expr, x_disp, y_disp,
          color_field=None, color_expr=None):
    fields = [{"name":x_field,"expression":x_expr},
              {"name":y_field,"expression":y_expr}]
    enc = {"x":{"fieldName":x_field,"displayName":x_disp,"scale":{"type":"categorical"}},
           "y":{"fieldName":y_field,"displayName":y_disp,"scale":{"type":"quantitative"}}}
    if color_field:
        fields.append({"name":color_field,"expression":color_expr})
        enc["color"] = {"fieldName":color_field,"displayName":color_field,"scale":{"type":"categorical"}}
    return {"widget":{"name":name,
        "queries":[{"name":"q","query":{"datasetName":ds,"fields":fields,"disaggregated":False}}],
        "spec":{"version":3,"widgetType":"bar","encodings":enc}},
        "position":{"x":x,"y":y,"width":w,"height":h}}

def table_w(name, x, y, w, h, ds, cols):
    fields = [{"name": fn, "expression": f"`{fn}`"} for fn, _ in cols]
    enc_cols = [{"fieldName": fn, "displayName": dn, "type": "string", "order": i}
                for i, (fn, dn) in enumerate(cols)]
    return {"widget":{"name":name,
        "queries":[{"name":"q","query":{"datasetName":ds,"fields":fields,"disaggregated":True}}],
        "spec":{"version":1,"widgetType":"table","encodings":{"columns":enc_cols},
        "invisibleColumns":[],"allowHTMLByDefault":False,"itemsPerPage":25}},
        "position":{"x":x,"y":y,"width":w,"height":h}}

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
    "pages": [{
        "name": "page1",
        "displayName": "Risk Officer",
        "layout": [
            text_w("title", 0, 0, 6, 1,
                   "# 💳 PinjamKilat — Credit Risk Dashboard\n_Bank Demo Sejahtera · synthetic Indonesian digital-loan data_"),
            counter_w("kpi_total", 0, 1, 2, 3, "ds_total", "total", "Total applications"),
            counter_pct("kpi_reject", 2, 1, 2, 3, "ds_reject_rate", "reject_rate", "Reject rate"),
            text_w("kpi_placeholder", 4, 1, 2, 3,
                   "## Decisions today\n\nUse the breakdowns below + the Genie space for drill-downs."),
            text_w("section_breakdowns", 0, 4, 6, 1, "## Breakdowns"),
            bar_w("by_province", 0, 5, 3, 5, "ds_by_province",
                  "provinsi", "n", "`provinsi`", "SUM(`n`)",
                  "Province", "Applications",
                  color_field="decision", color_expr="`decision`"),
            table_w("reasons", 3, 5, 3, 5, "ds_reasons",
                    [("reason","Reason code"), ("n","Count")]),
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
