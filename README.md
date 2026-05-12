# PinjamKilat Credit Decisioning — Databricks Workshop

A 2.5-hour hands-on workshop for **technical banking audiences**, built around a fictional Indonesian digital-lending product called **PinjamKilat** (operated by the fictional "Bank Demo Sejahtera"). Showcases how AI Functions on Indonesian text + AI/BI Genie turn messy loan-application data into an analyst-ready decisioning layer.

**Runs entirely on Databricks Free Edition.** No paid features required.

---

## Audience + format

- **Audience:** technical (data engineers, ML engineers, analytics engineers) at a digital bank or fintech lender. Familiar with SQL and Python; light Spark experience helpful.
- **Format:** presenter-led, hands-on. 2.5 hours, five notebooks (~25–35 min each).
- **Pacing:** each notebook has 1–2 *Your Turn* cells; solution cells immediately follow so anyone can catch up.

## Workshop arc

| # | Notebook | Time | What you'll do |
|---|---|---|---|
| 0 | [`00_setup.py`](notebooks/00_setup.py) | 20 min | Create UC catalog + schemas. Generate synthetic Indonesian loan applications (KTP OCR strings, Bahasa free-text application fields, call-center notes, 12-month transaction history). Land in `workshop.bronze`. |
| 1 | [`01_ai_extract_kyc.py`](notebooks/01_ai_extract_kyc.py) | 30 min | `ai_extract` to pull `nik`, `place_of_birth`, `dob`, `gender` from messy KTP OCR strings + `occupation_category`, `employer_sector`, `income`, `tenure_years` from Bahasa application free-text. |
| 2 | [`02_ai_classify_risk.py`](notebooks/02_ai_classify_risk.py) | 30 min | `ai_classify` employment stability. `ai_analyze_sentiment` on call-center notes. `ai_query` to synthesize a Bahasa risk narrative per applicant. |
| 3 | [`03_silver_gold_decisioning.py`](notebooks/03_silver_gold_decisioning.py) | 25 min | Combine extracted + classified signals with hard rules (DBR, age, blacklist) into `workshop.gold.vw_application_decisioning`. One row per application with `risk_score`, `decision`, `decision_reason_codes`. |
| 4 | [`04_applicant_360_features.py`](notebooks/04_applicant_360_features.py) | 25 min | Foundational data engineering: window-function aggregates over 12-month transactions, percentile-based features, CTEs, data-quality checks. Builds `workshop.gold.vw_applicant_360` — the second view Genie will see. |
| 5 | [`05_risk_genie.py`](notebooks/05_risk_genie.py) | 40 min | Productionised Genie: both gold views, 6 certified Q→SQL pairs (`example_question_sqls`), 5 benchmark Q+A test cases, automated benchmark runner that times and grades Genie's responses. AI/BI dashboard included. |

## Prereqs (5 min, before workshop day)

You need a **Databricks Free Edition** workspace (or any workspace with serverless SQL + AI Functions enabled).

1. Sign up at https://www.databricks.com/learn/free-edition if you don't have one.
2. Confirm you can access **Catalog** and **SQL Warehouses** in the left nav.
3. (Optional) A serverless SQL warehouse is started — Free Edition auto-creates one.

No CLI install, no Terraform, no DAB required for attendees.

## Attendee install — one-time

1. In your workspace, open the **Workspace** browser → click your username → **Import** → **URL**.
2. Paste each notebook URL (one at a time, or all five up front):

   | # | Import URL |
   |---|---|
   | 0 | `https://raw.githubusercontent.com/danteliew6/pinjamkilat-workshop/main/notebooks/00_setup.py` |
   | 1 | `https://raw.githubusercontent.com/danteliew6/pinjamkilat-workshop/main/notebooks/01_ai_extract_kyc.py` |
   | 2 | `https://raw.githubusercontent.com/danteliew6/pinjamkilat-workshop/main/notebooks/02_ai_classify_risk.py` |
   | 3 | `https://raw.githubusercontent.com/danteliew6/pinjamkilat-workshop/main/notebooks/03_silver_gold_decisioning.py` |
   | 4 | `https://raw.githubusercontent.com/danteliew6/pinjamkilat-workshop/main/notebooks/04_applicant_360_features.py` |
   | 5 | `https://raw.githubusercontent.com/danteliew6/pinjamkilat-workshop/main/notebooks/05_risk_genie.py` |

3. Open `00_setup.py` and **Run All**. It creates `workshop.{bronze,silver,gold}` and populates bronze.

## Maintainer install — DAB

If you'd rather deploy from a clone:

```bash
git clone https://github.com/danteliew6/pinjamkilat-workshop.git
cd pinjamkilat-workshop
# Set DATABRICKS_TF_EXEC_PATH if you hit the Terraform GPG issue.
databricks bundle deploy -t dev -p <your-profile>
```

The bundle uploads notebooks to `/Workspace/Users/<you>/.bundle/pinjamkilat-workshop/dev/files/notebooks/` and creates a runnable Job.

## Structure

```
pinjamkilat-workshop/
├── README.md                    # you are here
├── databricks.yml               # DAB main config
├── resources/
│   ├── jobs.yml                 # optional Job to run all notebooks sequentially
│   └── volumes.yml              # workshop.bronze.documents volume
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_ai_extract_kyc.py
│   ├── 02_ai_classify_risk.py
│   ├── 03_silver_gold_decisioning.py
│   ├── 04_applicant_360_features.py
│   └── 05_risk_genie.py
└── data/                        # (optional) sample static reference files
```

## Key design choices

- **Single catalog `workshop`** — survives Free Edition's single-catalog default; rename via the `CATALOG` constant at the top of every notebook if your workspace requires a different name.
- **Foundation model:** `databricks-claude-sonnet-4-5` (default; switch to `databricks-meta-llama-3-3-70b-instruct` if you prefer open-weights).
- **Synthetic data is regenerated every time** `00_setup.py` runs — fully deterministic via `np.random.default_rng(42)`, so the demo answers are stable across runs.
- **Solutions are visible.** Each *Your Turn* cell is followed by a clearly marked *Solution* cell so attendees who fall behind can copy-paste and move on.

## Disclaimer

"Bank Demo Sejahtera" and "PinjamKilat" are fictional. All names, NIK, transactions, and call-center notes are synthetic. No connection to any real Indonesian financial institution.
