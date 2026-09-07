"""
Orders ELT Pipeline — Extract + Validate + Load + dbt Transform Stages
-----------------------------------------------------------------------
Stage 1 (extract): pulls raw data from both sources and lands it on
disk for validation to pick up.

  extract_orders     -> calls the mock orders API, saves raw JSON
  extract_customers  -> reads the static customers.csv reference file

Stage 2 (validate): checks data quality before anything gets loaded
downstream. This is a STRICT validator — any failed check raises an
exception, which fails the task and stops the DAG. Nothing gets
passed to load/transform unless the data is clean.

  validate_data -> depends on both extract tasks completing first

Stage 3 (load): pure extract-and-load, no transformation. Writes the
raw orders JSON and raw customers CSV into two simple Postgres
tables (raw_orders, raw_customers). This is deliberately "dumb" —
all transformation logic now lives in dbt, not here.

  load_raw_data -> depends on validate_data passing

Stage 4 (transform, via dbt): dbt reads raw_orders/raw_customers,
joins them, and builds enriched_orders + revenue_by_region as dbt
models — replacing the Python join/aggregate logic entirely.

  run_dbt_transform -> depends on load_raw_data
  run_dbt_test      -> depends on run_dbt_transform; runs as a
                        SEPARATE task so a failed data-quality test
                        shows up distinctly from a broken transform
                        in the Grid view

Stage 5 (alerting): a failure callback attached to every task via
default_args, so ANY task failing anywhere in the DAG triggers an
alert (logged + emailed) automatically — no need to notice a red
square in the UI.

  alert_on_failure -> not a task node; fires automatically on
                       failure of any task in this DAG
"""

import csv
import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ruff (LOG015) flags calls on the root logger (logging.info/error)
# since they bypass per-module log configuration. Using a named
# logger scoped to this module is the standard fix.
logger = logging.getLogger(__name__)

# Paths inside the container. These map to ./data on the host via the
# volume mount added to x-airflow-common in docker-compose.yaml.
DATA_DIR = "/opt/airflow/data"
RAW_ORDERS_PATH = f"{DATA_DIR}/raw_orders.json"
CUSTOMERS_CSV_PATH = f"{DATA_DIR}/customers.csv"

# Paths inside the container for the dbt project + its
# container-specific profiles.yml (mounted in docker-compose.yaml).
DBT_PROJECT_DIR = "/opt/airflow/orders_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

# Service name from docker-compose.yaml — resolved via Docker's
# internal DNS since both containers share the default network.
MOCK_API_URL = "http://mock-orders-api:8000/orders"

# Postgres connection — same container/credentials Airflow's own
# metadata database uses (see AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
# in docker-compose.yaml). raw_orders/raw_customers are added as new
# tables in that same database rather than standing up a second one.
POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

ALERT_EMAIL = os.environ.get("ALERT_EMAIL")


def extract_orders():
    """Calls the mock orders API and writes the raw response to disk."""
    response = requests.get(MOCK_API_URL, params={"count": 50}, timeout=10)
    response.raise_for_status()
    payload = response.json()

    with open(RAW_ORDERS_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    logger.info("Extracted %d orders to %s", payload["count"], RAW_ORDERS_PATH)


def extract_customers():
    """Confirms the static customers.csv reference file is readable."""
    with open(CUSTOMERS_CSV_PATH, "r") as f:
        line_count = sum(1 for _ in f) - 1  # subtract header row

    logger.info("Found %d customer records at %s", line_count, CUSTOMERS_CSV_PATH)


def validate_data():
    """
    Strict data quality gate. Loads both raw files and checks:
      - orders JSON parses and every order has the required fields
      - every order's customer_id exists in customers.csv

    Any failed check raises, which fails this task and stops the DAG
    — nothing downstream (load/transform) runs on bad data.
    """
    with open(RAW_ORDERS_PATH, "r") as f:
        payload = json.load(f)
    orders = payload["orders"]

    with open(CUSTOMERS_CSV_PATH, "r") as f:
        customer_ids = {row["customer_id"] for row in csv.DictReader(f)}

    required_fields = {"order_id", "customer_id", "amount", "order_date", "status"}
    for order in orders:
        missing = required_fields - order.keys()
        if missing:
            raise ValueError(f"Order {order.get('order_id')} missing fields: {missing}")
        if str(order["customer_id"]) not in customer_ids:
            raise ValueError(
                f"Order {order['order_id']} references unknown customer_id: "
                f"{order['customer_id']}"
            )

    logger.info("Validation passed: %d orders checked, all clean.", len(orders))


CREATE_RAW_ORDERS_SQL = """
DROP TABLE IF EXISTS raw_orders;
CREATE TABLE raw_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount NUMERIC NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status TEXT NOT NULL
);
"""

CREATE_RAW_CUSTOMERS_SQL = """
DROP TABLE IF EXISTS raw_customers;
CREATE TABLE raw_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    region TEXT NOT NULL,
    segment TEXT NOT NULL
);
"""


def load_raw_data():
    """
    Pure extract-and-load — no transformation. Loads the already-
    validated raw_orders.json and customers.csv straight into two
    Postgres tables (raw_orders, raw_customers), so dbt has real
    source tables to build models from.

    Both tables are dropped and recreated each run (full refresh),
    keeping re-runs of the DAG idempotent.
    """
    with open(RAW_ORDERS_PATH, "r") as f:
        orders = json.load(f)["orders"]

    with open(CUSTOMERS_CSV_PATH, "r") as f:
        customers = list(csv.DictReader(f))

    conn = psycopg2.connect(**POSTGRES_CONN)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_RAW_ORDERS_SQL)
            cur.execute(CREATE_RAW_CUSTOMERS_SQL)

            for o in orders:
                cur.execute(
                    """
                    INSERT INTO raw_orders
                        (order_id, customer_id, amount, order_date, status)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (o["order_id"], o["customer_id"], o["amount"], o["order_date"], o["status"]),
                )

            for c in customers:
                cur.execute(
                    """
                    INSERT INTO raw_customers
                        (customer_id, customer_name, region, segment)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (c["customer_id"], c["name"], c["region"], c["segment"]),
                )
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Loaded %d rows into raw_orders, %d rows into raw_customers.",
        len(orders), len(customers),
    )


def alert_on_failure(context):
    """
    Failure callback attached to every task via default_args below.
    Airflow calls this automatically whenever a task fails, passing
    a `context` dict with details about the failed run.

    Logs a clear ALERT line and, since email_on_failure is also set
    below, Airflow separately sends a real email via the configured
    SMTP connection.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")

    logger.error(
        "ALERT: task '%s' failed in dag '%s' (run_id=%s). Exception: %s",
        task_instance.task_id,
        task_instance.dag_id,
        context["run_id"],
        exception,
    )


with DAG(
    dag_id="orders_elt_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
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

    load_raw_data_task = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
    )

    run_dbt_transform_task = BashOperator(
        task_id="run_dbt_transform",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    run_dbt_test_task = BashOperator(
        task_id="run_dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # extract_orders and extract_customers run in parallel. validate_data
    # waits for both. load_raw_data only runs once validation has passed.
    # dbt takes over from there: transform first, then test as a
    # separate downstream task so a failed test is visibly distinct
    # from a broken transform in the Grid view.
    [extract_orders_task, extract_customers_task] >> validate_data_task
    validate_data_task >> load_raw_data_task >> run_dbt_transform_task >> run_dbt_test_task