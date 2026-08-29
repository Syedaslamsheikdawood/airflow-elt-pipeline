"""
Orders ELT Pipeline — Extract + Validate + Transform/Load Stages
--------------------------------------------------------------------
Stage 1 (extract): pulls raw data from both sources and lands it on
disk for validation to pick up.

  extract_orders     -> calls the mock orders API, saves raw JSON
  extract_customers  -> reads the static customers.csv reference file

Stage 2 (validate): checks data quality before anything gets loaded
downstream. This is a STRICT validator — any failed check raises an
exception, which fails the task and stops the DAG. Nothing gets
passed to transform/load unless the data is clean.

  validate_data -> depends on both extract tasks completing first

Stage 3 (transform + load): joins orders to customers in Python,
then writes two tables into Postgres:
  - enriched_orders    : one row per order, with customer info attached
  - revenue_by_region  : total amount + order count, grouped by
                         region and segment

Each run TRUNCATEs and re-inserts both tables (full refresh), so
re-running the DAG is idempotent — no duplicate rows pile up.

  transform_load -> depends on validate_data passing

Stage 4 (alerting): a failure callback attached to every task via
default_args, so ANY task failing anywhere in the DAG logs a clear
alert automatically — no need to notice a red square in the UI.
This currently logs the alert; real email delivery gets wired in
as a follow-up once SMTP is configured.

  alert_on_failure -> not a task node; fires automatically on
                       failure of any task in this DAG
"""

import csv
import json
import logging
import os
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Paths inside the container. These map to ./data on the host via the
# volume mount added to x-airflow-common in docker-compose.yaml.
DATA_DIR = "/opt/airflow/data"
RAW_ORDERS_PATH = f"{DATA_DIR}/raw_orders.json"
CUSTOMERS_CSV_PATH = f"{DATA_DIR}/customers.csv"

# Service name from docker-compose.yaml — resolved via Docker's
# internal DNS since both containers share the default network.
MOCK_API_URL = "http://mock-orders-api:8000/orders"

# Postgres connection — same container/credentials Airflow's own
# metadata database uses (see AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
# in docker-compose.yaml). We're adding new tables to the same
# database rather than standing up a second one.
POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# Read from the container environment (set via ALERT_EMAIL in
# docker-compose.yaml, sourced from .env) rather than hardcoding an
# address here — this file gets pushed to a public repo.
ALERT_EMAIL = os.environ.get("ALERT_EMAIL")


def extract_orders():
    """Calls the mock orders API and writes the raw response to disk."""
    response = requests.get(MOCK_API_URL, params={"count": 50}, timeout=10)
    response.raise_for_status()
    payload = response.json()

    with open(RAW_ORDERS_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    logging.info("Extracted %d orders to %s", payload["count"], RAW_ORDERS_PATH)


def extract_customers():
    """Confirms the static customers.csv reference file is readable."""
    with open(CUSTOMERS_CSV_PATH, "r") as f:
        line_count = sum(1 for _ in f) - 1  # subtract header row

    logging.info("Found %d customer records at %s", line_count, CUSTOMERS_CSV_PATH)


REQUIRED_ORDER_FIELDS = ["order_id", "customer_id", "amount", "order_date", "status"]


def validate_data():
    """
    Strict data-quality gate. Raises an exception (failing the task)
    on the first category of problem found, so bad data never reaches
    the transform/load stage.

    Checks:
      1. Every order has all required fields, non-null
      2. No duplicate order_ids within the batch
      3. Every order's customer_id exists in customers.csv
      4. amount is a positive number
      5. order_date parses as a valid datetime
    """
    with open(RAW_ORDERS_PATH, "r") as f:
        orders = json.load(f)["orders"]

    with open(CUSTOMERS_CSV_PATH, "r") as f:
        known_customer_ids = {int(row["customer_id"]) for row in csv.DictReader(f)}

    seen_order_ids = set()

    for order in orders:
        # 1. required fields present and non-null
        missing = [field for field in REQUIRED_ORDER_FIELDS if order.get(field) in (None, "")]
        if missing:
            raise ValueError(f"Order {order.get('order_id')} missing fields: {missing}")

        # 2. duplicate order_id check
        order_id = order["order_id"]
        if order_id in seen_order_ids:
            raise ValueError(f"Duplicate order_id found: {order_id}")
        seen_order_ids.add(order_id)

        # 3. referential integrity against customers.csv
        customer_id = order["customer_id"]
        if customer_id not in known_customer_ids:
            raise ValueError(
                f"Order {order_id} references unknown customer_id: {customer_id}"
            )

        # 4. amount must be a positive number
        amount = order["amount"]
        if not isinstance(amount, (int, float)) or amount <= 0:
            raise ValueError(f"Order {order_id} has invalid amount: {amount}")

        # 5. order_date must parse
        try:
            datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(
                f"Order {order_id} has unparseable order_date: {order['order_date']}"
            )

    logging.info("Validation passed: %d orders checked, all clean.", len(orders))


CREATE_ENRICHED_ORDERS_SQL = """
CREATE TABLE IF NOT EXISTS enriched_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name TEXT NOT NULL,
    region TEXT NOT NULL,
    segment TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status TEXT NOT NULL
);
"""

CREATE_REVENUE_BY_REGION_SQL = """
CREATE TABLE IF NOT EXISTS revenue_by_region (
    region TEXT NOT NULL,
    segment TEXT NOT NULL,
    total_amount NUMERIC NOT NULL,
    order_count INTEGER NOT NULL,
    PRIMARY KEY (region, segment)
);
"""


def transform_load():
    """
    Joins orders to customers, then loads two tables into Postgres:
      - enriched_orders: one row per order with customer info attached
      - revenue_by_region: total amount + order count per region/segment

    Both tables are truncated and re-inserted each run (full refresh),
    so re-running the DAG doesn't create duplicate rows.
    """
    with open(RAW_ORDERS_PATH, "r") as f:
        orders = json.load(f)["orders"]

    with open(CUSTOMERS_CSV_PATH, "r") as f:
        customers_by_id = {int(row["customer_id"]): row for row in csv.DictReader(f)}

    # --- join: attach customer info to each order ---
    enriched_rows = []
    for order in orders:
        customer = customers_by_id[order["customer_id"]]
        enriched_rows.append((
            order["order_id"],
            order["customer_id"],
            customer["name"],
            customer["region"],
            customer["segment"],
            order["amount"],
            order["order_date"],
            order["status"],
        ))

    # --- aggregate: total amount + count per region/segment ---
    aggregates = {}
    for _, _, _, region, segment, amount, _, _ in enriched_rows:
        key = (region, segment)
        totals = aggregates.setdefault(key, {"total_amount": 0.0, "order_count": 0})
        totals["total_amount"] += amount
        totals["order_count"] += 1

    aggregate_rows = [
        (region, segment, round(totals["total_amount"], 2), totals["order_count"])
        for (region, segment), totals in aggregates.items()
    ]

    # --- load into Postgres ---
    conn = psycopg2.connect(**POSTGRES_CONN)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_ENRICHED_ORDERS_SQL)
            cur.execute(CREATE_REVENUE_BY_REGION_SQL)

            cur.execute("TRUNCATE TABLE enriched_orders;")
            cur.executemany(
                """
                INSERT INTO enriched_orders
                    (order_id, customer_id, customer_name, region, segment,
                     amount, order_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                enriched_rows,
            )

            cur.execute("TRUNCATE TABLE revenue_by_region;")
            cur.executemany(
                """
                INSERT INTO revenue_by_region
                    (region, segment, total_amount, order_count)
                VALUES (%s, %s, %s, %s);
                """,
                aggregate_rows,
            )
        conn.commit()
    finally:
        conn.close()

    logging.info(
        "Loaded %d enriched orders and %d region/segment aggregates.",
        len(enriched_rows), len(aggregate_rows),
    )


def alert_on_failure(context):
    """
    Failure callback attached to every task via default_args below.
    Airflow calls this automatically whenever a task fails, passing
    a `context` dict with details about the failed run.

    Currently logs a clear ALERT line — this is the hook point where
    real email/Slack/PagerDuty delivery gets plugged in later; the
    calling pattern (Airflow invokes this on any failure) doesn't
    change, only what happens inside the function does.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")

    logging.error(
        "ALERT: task '%s' failed in dag '%s' (run_id=%s). Exception: %s",
        task_instance.task_id,
        task_instance.dag_id,
        context["run_id"],
        exception,
    )


with DAG(
    dag_id="orders_elt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",  # runs automatically once per day
    catchup=False,
    default_args={
        "on_failure_callback": alert_on_failure,
        "email_on_failure": True,
        "email": [ALERT_EMAIL] if ALERT_EMAIL else [],
    },
) as dag:

    extract_orders_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    extract_customers_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform_load_task = PythonOperator(
        task_id="transform_load",
        python_callable=transform_load,
    )

    # extract_orders and extract_customers run in parallel (no
    # dependency between them). validate_data waits for BOTH to
    # finish before running, since it needs both files. transform_load
    # only runs once validation has passed.
    [extract_orders_task, extract_customers_task] >> validate_data_task >> transform_load_task