# StepRight Lakehouse

An end-to-end data engineering project on **Databricks (AWS)**. It builds a medallion lakehouse (Bronze → Silver → Gold) for **StepRight**, a fictional online footwear retailer. The project covers CDC ingestion, data-quality-as-code, SCD Type 1 and Type 2 history, business-ready Gold tables, monitoring, and a full CI/CD flow across **dev → uat → prod**.

<img width="2760" height="1770" alt="architecture" src="https://github.com/user-attachments/assets/9bc7697c-0c8b-4d29-b6bc-d121caa77956" />


---

## Highlights

- **Two Lakeflow Declarative Pipelines (serverless).** One handles ingestion (Bronze), one handles transformation (Silver + Gold).
- **CDC handling.** Debezium-style change events (`op`, `ts_ms`, `before`, `after`) are kept raw in Bronze and applied with `AUTO CDC` in Silver.
- **SCD Type 2** history for orders and customers. **SCD Type 1** for order items, products and categories.
- **Quarantine pattern.** Bad rows are tagged and routed to `*_quarantined` tables. They are never silently dropped and never block the pipeline.
- **Data quality at three levels:** row-level expectations, a job-level DQ gate, and real-time alert hooks on the pipeline event log.
- **DQ dashboard** (Lakeview) built on the pipeline event logs, with tabs for expectations, quarantine trends and orphan records.
- **Environment-agnostic code.** The same code is deployed to `dev`, `uat` and `prod` with **Databricks Asset Bundles**. Only the Unity Catalog catalog name changes.
- **Testing at every stage:** local PySpark unit tests, an end-to-end integration test on UAT, and read-only smoke tests on prod.
- **GitHub Actions CI/CD**, with a manual, confirmation-gated production deploy.

---

## Tech stack

| Area | Tools |
|---|---|
| Platform | Databricks on AWS, Unity Catalog, UC Volumes, serverless compute |
| Pipelines | Lakeflow Declarative Pipelines (`pyspark.pipelines`), Auto Loader, `AUTO CDC` |
| Language | Python, PySpark, Spark SQL |
| Orchestration | Databricks Jobs |
| Monitoring | Pipeline event log, `@dp.on_event_hook`, Lakeview dashboard |
| Deployment | Databricks Asset Bundles (`databricks bundle`) |
| CI/CD | GitHub Actions |
| Testing | pytest, local PySpark, Databricks SDK |
| Test data | Python + Faker (seeded, deterministic generator) |

---

## Business questions answered (Gold layer)

| Gold table | Stakeholder | What it answers |
|---|---|---|
| `gold_daily_revenue` | Finance | Gross revenue, discount and net revenue by day, category and region. Order-level discounts are split across line items by their share of the order. |
| `gold_customer_360` | Marketing | Lifetime value, order count, last order date and days since last order (a simple churn signal). |
| `gold_product_performance` | Merchandising | Top sellers by units and revenue, plus stockout risk from the latest inventory snapshot. |
| `gold_funnel_analysis` | Growth | Conversion rate by referrer, measured per session so anonymous visitors are included. |
| `gold_fulfillment_health` | Operations | Delivery SLA compliance (default 5 days) by day and region. |

---

## Data sources

Seven sources are simulated by `data_generator.py`:

| Source | Type | Format |
|---|---|---|
| orders | CDC (Debezium envelope) | JSON |
| order_items | CDC (Debezium envelope) | JSON |
| customers | CDC (Debezium envelope) | JSON |
| products | File drop (catalog export) | CSV |
| categories | File drop (reference data) | CSV |
| clickstream | Event stream | JSON |
| inventory | Daily snapshot per warehouse | CSV |

The generator creates a full snapshot (`batch_0`) and incremental batches (`batch_1`, `batch_2`, …) with new rows, status updates, profile changes and price changes. It keeps referential integrity across batches using `generator_state.json`. With `--seed` and `--as-of` it produces exactly the same output every time, which is what the integration tests use.

---

## Architecture

### 1. Landing
Generated batches are uploaded to a `staging` volume. `setup/02_data_loader.py` then copies them into `/Volumes/<catalog>/stepright/landing/<source>/` and checks row counts.

### 2. Bronze — `stepright-ingestion-pipeline`
- **Auto Loader** reads every source with an **explicit schema** (no schema inference, since these are known, contract-bound sources). It adds `_ingested_at` and `_source_file` to each row.
- CDC tables keep the full Debezium envelope exactly as it arrived, so any rebuild can be replayed from Bronze.
- Each Bronze table goes through a private `*_quality_check` table that applies **structural rules** with `@dp.expect_all` (for example, IDs not null, amounts not negative) and adds an `is_quarantined` flag.
- Rows are split into published `*_valid` and `*_quarantined` tables (7 of each). Silver only reads from `*_valid`.

### 3. Silver — `stepright-transformation-pipeline`
- `*_change_feed` views flatten the CDC envelope and cast strings to proper types (timestamps, dates).
- `AUTO CDC` flows handle ordering, deduplication and out-of-order events:
  - `silver_orders`, `silver_customers` → **SCD Type 2** (full history of status and loyalty-tier changes)
  - `silver_products`, `silver_categories` → **SCD Type 1** (primary key constraints declared)
  - `silver_order_items_all` → **SCD Type 1**
- `silver_clickstream` and `silver_inventory` are append-only streams.
- **Business rules** (for example, valid order status, retail price above cost, customers aged 18+) are applied as report-only expectations.
- One rule is enforced: if `line_total ≠ quantity × unit_price`, the order item goes to quarantine. A wrong number here would flow straight into Gold revenue sums. Gold only reads the clean `silver_order_items` table.

### 4. Gold
Five materialized views (see the table above). SCD Type 2 tables are filtered to their current row (`__END_AT IS NULL`) before any aggregation.

All Gold logic lives in plain `*_logic.py` modules with no pipeline import. The pipeline files are thin wrappers. This keeps the logic **unit-testable with local PySpark**, with no Databricks needed. Values like "today", the SLA days and the stockout threshold are passed in as parameters so the tests stay deterministic.

### 5. Orchestration — `stepright-orchestration-job`

```
run_ingestion  →  dq_check  →  run_transformation  →  report
```

- `dq_check` is a **job-level gate**. It checks that every Bronze table received rows for `run_date` and that no quarantine rate is above 50%. If either check fails, transformation never starts.
- `report.py` prints row counts for every Bronze, Silver and Gold table, plus quarantine counts, into the job log.

### 6. Monitoring and data quality

| Level | Where | Behaviour |
|---|---|---|
| Row-level (structural) | Bronze `expect_all` | Tag and quarantine, never block |
| Row-level (business) | Silver `expect_all` | Report only (except `line_total`, which is quarantined) |
| Job-level | `jobs/dq_check.py` | Stops the run if data is missing or quarantine spikes |
| Real-time | `dq_alert_hook.py` in both pipelines | Alerts when any expectation fails on more than 25% of rows |
| Trends | StepRight DQ Dashboard | Expectations history, quarantine counts, referential-integrity orphans |

`setup/03_event_log_setup.py` creates one `event_log()` view per pipeline. `monitoring/dq_dashboard_setup.py` builds the three views behind the dashboard: `dq_expectations_history`, `dq_quarantine_counts` and `dq_referential_integrity`.

Foreign keys are monitored with anti-joins instead of being declared as constraints.

---

## Pipeline graph

The live graph of the transformation pipeline in Databricks:

<img width="1327" height="752" alt="pipeline_combined" src="https://github.com/user-attachments/assets/f03d8e91-ed5b-4e66-b1fb-cef8d1db9db2" />


---

## CI/CD

Everything is deployed with **Databricks Asset Bundles** (`bundle/databricks.yml`). There are three targets: `dev`, `uat` and `prod`. The catalog name is injected per target through the `stepright.catalog` pipeline setting, so the code itself never changes between environments.

**`ci.yml`** (UAT)
1. **unit-test**: runs the Bronze and Gold pytest suites with local PySpark.
2. **validate-and-deploy-uat**: creates the `uat` catalog if needed, validates and deploys the bundle, then runs the environment bootstrap job (setup → ingestion → transformation → event log views → DQ views).
3. **integration-test**: loads seeded, deterministic data and runs the orchestration job end to end on UAT.

**`deploy-prod.yml`** (Production)
- Triggered manually only. You must type `DEPLOY PROD` exactly to approve.
- Validates and deploys the bundle to prod, runs the idempotent bootstrap, then runs **read-only smoke tests** (resources reachable, latest pipeline updates not failed, dashboard exists, Gold tables queryable).

---

## Project structure

```
stepright-lakehouse/
├── .github/workflows/
│   ├── ci.yml                      # unit tests → deploy uat → integration tests
│   └── deploy-prod.yml             # manual, confirmation-gated prod deploy
├── bundle/
│   ├── databricks.yml              # bundle config, variables, dev/uat/prod targets
│   └── resources/                  # pipelines, jobs and dashboard definitions
├── pipelines/
│   ├── ingestion_pipeline/
│   │   ├── transformations/        # bronze_cdc, bronze_files, bronze_quality, dq_alert_hook
│   │   ├── utilities/              # helpers, dq_alert_logic
│   │   └── tests/                  # Bronze unit tests
│   └── transformation_pipeline/
│       ├── transformations/        # silver_cdc, silver_files, gold_* (+ *_logic), dq_alert_hook
│       ├── utilities/
│       └── tests/                  # Gold unit tests
├── jobs/
│   ├── dq_check.py                 # job-level DQ gate
│   └── report.py                   # post-run summary
├── monitoring/
│   ├── dq_dashboard_setup.py       # DQ views
│   └── dq_dashboard.lvdash.json    # Lakeview dashboard
├── setup/
│   ├── 01_environment_setup.py     # catalog, schema, volumes
│   ├── 02_data_loader.py           # staging → landing
│   └── 03_event_log_setup.py       # event log views
├── tests/
│   ├── integration/                # end-to-end test on UAT
│   └── smoke/                      # read-only prod checks
├── raw_sources/batch_0/            # sample generated batch
├── data_generator.py
└── requirements.txt
```

---

## Getting started

### Prerequisites
- A Databricks workspace with Unity Catalog and serverless enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) installed and authenticated
- Python 3.12

### 1. Generate data
```bash
pip install faker
python data_generator.py --batch 0 --output-dir ./raw_sources
# later, for incremental loads
python data_generator.py --batch 1 --output-dir ./raw_sources
```

### 2. Deploy to dev
Update the workspace host and `stepright_warehouse_id` in `bundle/databricks.yml`, then:
```bash
cd bundle
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run stepright_environment_bootstrap --target dev
```

### 3. Load a batch
Upload `raw_sources/batch_0/` to the `<catalog>.stepright.staging` volume, then run `setup/02_data_loader.py` with `batch = 0`.

### 4. Run the pipeline
```bash
databricks bundle run stepright_orchestration_job --target dev
```

### 5. Run the unit tests locally
```bash
pip install pytest "pyspark>=3.5"
pytest pipelines/ingestion_pipeline/tests -v
pytest pipelines/transformation_pipeline/tests -v
```

### GitHub secrets for CI/CD
| Secret | Used for |
|---|---|
| `DATABRICKS_HOST_UAT`, `DATABRICKS_TOKEN_UAT` | dev / uat workspace |
| `DATABRICKS_WAREHOUSE_ID_UAT` | creating the `uat` catalog |
| `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD` | prod workspace |

---

## Key design decisions

- **Bronze stays raw.** No flattening or business logic, so any downstream table can be rebuilt without touching the source systems.
- **Explicit schemas instead of inference** for known, internal sources.
- **Tag instead of drop.** `expect_all` keeps every row visible. Failing the pipeline on one bad row, or silently losing it, are both avoided.
- **Structural checks in Bronze, business checks in Silver.** Rules that need the full, flattened row belong in Silver.
- **Separate logic from pipeline code** so transformations can be unit-tested without a cluster.
- **One codebase, many environments.** The catalog is a parameter, never hardcoded.
- **Prod deploy is a deliberate human action**, not an automatic push.

---

## Possible improvements
- Replace the stubbed `send_alert()` with a real Slack or PagerDuty webhook using Databricks secrets.
- Replace the manual staging upload with a real CDC connector (for example, Debezium to cloud storage).
- Schedule the orchestration job and add freshness SLAs.
