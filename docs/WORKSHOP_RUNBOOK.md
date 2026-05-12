# Workshop runbook — PinjamKilat Credit Decisioning

**Audience:** Amar Bank technical team (data engineers, ML engineers, analytics engineers).
**Length:** 2.5 hours.
**Format:** presenter-led, hands-on. Attendees have a Free Edition workspace open and follow along.

## Pre-flight (the night before)

- [ ] Confirm every attendee has signed up for Databricks Free Edition and can log in.
- [ ] Send a single email with: the GitHub repo URL, the import-from-URL instructions, and your contact for day-of issues.
- [ ] Optional: pre-import notebook `00_setup.py` into a shared folder so anyone who lags behind can clone it.

## Day-of pacing — 3 hours (with 5-min mid-break)

| Time | Block | What to do |
|---|---|---|
| 00:00 – 00:10 | Welcome + framing | One slide. Why digital lenders care about Databricks AI Functions + Genie. Acknowledge that we're synthetic-data-only ("Bank Demo Sejahtera / PinjamKilat"). |
| 00:10 – 00:30 | **`00_setup.py`** | Everyone imports + Run All. While it runs, walk through what's in each table. Highlight the messy free-text — *that's what we're going to untangle*. |
| 00:30 – 01:00 | **`01_ai_extract_kyc.py`** | Live-run the KTP extract together. Pause after the first display() — let people look at the JSON output. Then occupation extract. Then both Your Turn cells (4 min each, then walk through). |
| 01:00 – 01:30 | **`02_ai_classify_risk.py`** | Crowd-pleaser. Demo `ai_analyze_sentiment` on a Bahasa call note. Then the *"stable on paper but negative call"* Your Turn — workshop's "aha" moment. The `ai_query` narrative is the closer. |
| 01:30 – 01:35 | **Break** | 5-min stretch. Tables/views from notebooks 1 & 2 are now built. |
| 01:35 – 02:00 | **`03_silver_gold_decisioning.py`** | Rules + audit trail. Frame as "AI signals + hard banking rules = audit-friendly decision". Run the SQL together; histogram-by-province Your Turn. |
| 02:00 – 02:25 | **`04_applicant_360_features.py`** | Data-engineering interlude. Walk through window-function tx aggregation + percentile features. Emphasize: **Genie can only answer questions about data it can see — feature engineering is what unlocks new questions**. If time-pressed, demo only (skip Your Turn cells). |
| 02:25 – 03:05 | **`05_risk_genie.py`** | Climax. Build the dashboard programmatically. Open the Genie URL — ask 2 sample questions live (one English + one Bahasa). Walk through `example_question_sqls` (certified queries) + `benchmarks.questions` — *this is what makes the Genie space a production prototype, not a toy*. Run the benchmark cell live. |
| 03:05 | Wrap | Show the gold view column comments — *that's why Genie works*. Take questions. |

### If you only have 2.5 hr

- Cut Module 4's Your Turn cells (still demo the view).
- Skip the benchmark runner in Module 5 (mention it exists, point to the cell).
- That recovers ~25 min.

## Demo questions to pre-warm in Genie

These all work on the synthetic data — try them yourself before the workshop:

1. *"Show me applications rejected in the last 14 days, broken down by reason code."*
2. *"Aplikasi mana yang ter-flag karena pendapatan tidak konsisten minggu ini?"* (Bahasa)
3. *"Which kabupaten has the highest reject rate? Top 5."*
4. *"Show me REJECT applications where employment_stability is 'stable'."*
5. *"Average DBR by employment_type, only APPROVE applications."*

## Talk-track cues

When introducing **`ai_extract`**: *"You don't write regex anymore. The model reads the field for you. It works on Bahasa, English, garbled OCR — the same call."*

When introducing **`ai_analyze_sentiment`**: *"Notice: we did not train this. There is no fine-tune. It runs on the foundation model API behind your serverless warehouse, billed per token."*

When introducing **Genie**: *"Genie reads the column COMMENT clauses you saw in the gold view DDL. That's why it knows what `is_blacklisted` means and what `decision_reason_codes` looks like as an ARRAY. Column comments are how you teach Genie."*

When the *"stable but rejected"* moment lands: *"This is the use case in one slide. Your existing credit model says 'stable, give the loan'. The call-center humans noticed something off. AI Functions on the call note + a SQL join make that human observation queryable."*

## Common questions to expect

| Question | Quick answer |
|---|---|
| "Does this work on Free Edition forever, or is there a quota?" | Free Edition includes a fair-use quota for AI Functions. For sustained production load, upgrade to a paid workspace. |
| "Which foundation model is being used?" | `databricks-claude-sonnet-4-5` in the workshop. Swap to `databricks-meta-llama-3-3-70b-instruct` if you want open-weights. |
| "Can we use our own model?" | Yes — `ai_query` accepts any model-serving endpoint name. Host your own via Mosaic AI Model Serving. |
| "What about Vector Search / RAG?" | Possible on paid tiers. Mosaic AI Vector Search isn't on Free Edition; you'd do RAG with chunked AI Functions calls instead. |
| "Why didn't we train a classical model?" | We're showing the AI-Function path because it's the new lever you may not have explored. Your team's existing classical models still apply; bolt them on as another column in the gold view. |
| "What about OJK / regulatory audit trails?" | `decision_reason_codes` is your audit trail. Every reason that fires is in the array; the underlying SQL is in the view DDL. |
| "How do we replace synthetic data with real?" | Replace bronze ingestion. Silver + gold + Genie work unchanged — that's the medallion benefit. |

## Troubleshooting cheat-sheet

- **"Cannot create catalog 'workshop'"** → tell attendees to change the `CATALOG` constant in `00_setup.py` to whatever catalog they *can* write to (often `main` or `<username>`). The rest of the workshop accepts that override.
- **`ai_extract` returns nulls everywhere** → SQL warehouse may not be a Pro/Serverless warehouse. Confirm warehouse type is Serverless.
- **Genie says "no tables"** → the gold view didn't build. Run notebook 3 again.
- **Dashboard creation fails with 400** → warehouse is stopped. Start the warehouse manually, re-run the dashboard cell.

## Cleanup

To reset between cohorts:

```sql
DROP CATALOG IF EXISTS workshop CASCADE;
```

Then re-run `00_setup.py`. Takes ~3 minutes.
