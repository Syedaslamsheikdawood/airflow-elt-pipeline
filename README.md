# Orders ELT Pipeline (Apache Airflow)

An end-to-end ELT pipeline built with Apache Airflow that extracts order data from a live API and a static customer reference file, validates it against a strict data-quality gate, transforms and loads it into Postgres, and sends real email alerts on failure.

This project simulates a realistic small-scale data engineering scenario: a fast-moving transactional feed (orders) joined against a slow-changing reference dataset (customers), with the kind of data-quality and failure-handling practices expected in production pipelines.

## Architecture

```
┌────────────────────┐        ┌──────────────────────┐
│  Mock Orders API    │        │   customers.csv      │
│  (FastAPI service)  │        │  (static reference)  │
└──────────┬───────────┘        └──────────┬────────────┘
           │                                │
           ▼                                ▼
    extract_orders                 extract_customers
           │                                │
           └───────────────┬────────────────┘
                            ▼
                     validate_data
                (strict fail on bad data)
                            │
                            ▼
                     transform_load
              (join + aggregate → Postgres)
                            │
                            ▼
              enriched_orders / revenue_by_region

  Any task failure anywhere in the DAG triggers:
    - a structured log alert (on_failure_callback)
    - a real email notification (SMTP)
```

## Tech Stack

- **Orchestration:** Apache Airflow 2.10 (LocalExecutor), Docker Compose
- **Mock upstream source:** FastAPI + Faker (simulates a live orders API)
- **Storage:** PostgreSQL
- **Language:** Python (requests, psycopg2, csv, json)
- **Alerting:** Airflow's built-in SMTP integration (Gmail)

## Key Features

- **Two independent source types, joined downstream** — a live, randomly-generated "orders" feed (API) and a static "customers" reference file (CSV), mirroring a common real-world fact/dimension pattern.
- **Strict data-quality gate.** `validate_data` checks for missing fields, duplicate order IDs, referential integrity (every order's `customer_id` must exist in the customer file), invalid amounts, and unparseable dates. Any failure raises an exception and **stops the pipeline** — bad data never reaches the load stage.
- **Idempotent loads.** `transform_load` truncates and re-inserts on every run, so re-running the DAG never creates duplicate rows.
- **Automatic failure alerting on every task**, wired once at the DAG level via `default_args` rather than repeated per task:
  - A structured log message (`on_failure_callback`) — the extensible hook point for Slack/PagerDuty/etc.
  - A real email sent via Gmail SMTP (`email_on_failure`), confirmed working end-to-end.
- **Scheduled to run autonomously** (daily), not just on manual trigger.

## Project Structure

```
airflow-elt-pipeline/
├── dags/
│   ├── hello_pipeline.py          # initial sanity-check DAG
│   └── orders_elt_pipeline.py     # main ELT pipeline
├── data/
│   └── customers.csv              # static reference dataset
├── mock-orders-api/
│   ├── main.py                    # FastAPI app serving fake orders
│   ├── requirements.txt
│   └── Dockerfile
├── logs/
├── plugins/
├── .env                           # SMTP credentials (not committed)
├── .gitignore
└── docker-compose.yaml
```

## Setup & Running Locally

**Prerequisites:** Docker Desktop, Python 3.12+ (for local editing only — the pipeline itself runs entirely in containers).

1. Clone the repo and `cd` into it.
2. Create a `.env` file in the project root with:
   ```
   SMTP_EMAIL=your-alerts-account@gmail.com
   SMTP_APP_PASSWORD=your16charapppassword
   ```
   (Requires a Gmail account with 2-Step Verification enabled and an [App Password](https://myaccount.google.com/apppasswords) generated for this project.)
3. Build and start the stack:
   ```
   docker compose up -d --build
   ```
4. Open the Airflow UI at [http://localhost:8081](http://localhost:8081) (login: `airflow` / `airflow`).
5. Unpause `orders_elt_pipeline` and trigger it manually, or let it run on its daily schedule.
6. Inspect results directly in Postgres:
   ```
   docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM enriched_orders LIMIT 5;"
   docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM revenue_by_region ORDER BY total_amount DESC;"
   ```

## Validation & Alerting — Proven, Not Just Claimed

This pipeline's failure handling was deliberately tested, not just written and assumed to work:

- **Negative test:** manually corrupted `raw_orders.json` with an unknown `customer_id`, confirming `validate_data` fails loudly with a clear exception and the DAG halts before loading bad data.
- **Alerting test:** confirmed the same failure produced both a structured log alert and a real email notification, delivered to an inbox within seconds.

*(Screenshots below)*

### Screenshots

**Successful pipeline run (all tasks green)**
![Successful pipeline run](screenshots/pipeline-success.png)

**`validate_data` failing intentionally on bad data**
![Validation failure](screenshots/validation-failure.png)

**Real failure alert email received**
![Email alert](screenshots/email-alert.png)

**Query results from `enriched_orders` and `revenue_by_region`**
![Postgres query results](screenshots/postgres-query-results.png)

## What I'd Improve Next

- Swap the strict-fail validator for a **quarantine pattern** — route bad records to a dead-letter table for inspection instead of halting the whole batch.
- Add incremental/CDC-style loading instead of full truncate-and-reload, once data volumes justify it.
- Add Slack notifications alongside email, to demonstrate the alerting hook is genuinely pluggable.
- Add automated tests (pytest) for the validation and transform logic, independent of Airflow itself.