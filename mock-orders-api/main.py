"""
Mock Orders API
----------------
Simulates a live upstream "orders" feed for the Airflow ELT pipeline.
Every call to /orders generates a fresh random batch of fake order
records, rather than serving a fixed static list — so repeated
Airflow DAG runs pull genuinely different data, just like a real
transactional system would produce new orders over time.
"""

import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from fastapi import FastAPI, Query

app = FastAPI(title="Mock Orders API")
fake = Faker()

# Fixed pool of customer IDs. This SAME range (1-50) is also used to
# generate customers.csv, so downstream referential-integrity checks
# (does every order's customer_id exist in the customer dimension?)
# have something real to validate against.
CUSTOMER_ID_POOL = list(range(1, 51))

# Weighted so the data looks like a real order distribution instead
# of being evenly random — most orders complete, some are still
# pending, a few get cancelled.
STATUS_WEIGHTS = {
    "completed": 0.70,
    "pending": 0.20,
    "cancelled": 0.10,
}


def _random_status() -> str:
    return random.choices(
        population=list(STATUS_WEIGHTS.keys()),
        weights=list(STATUS_WEIGHTS.values()),
        k=1,
    )[0]


def _random_order_date() -> str:
    # Orders spread across the last 30 days, to look like a rolling feed.
    days_ago = random.randint(0, 30)
    order_dt = datetime.now(timezone.utc) - timedelta(
        days=days_ago,
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return order_dt.strftime("%Y-%m-%d %H:%M:%S")


def _generate_order(order_id: int) -> dict:
    return {
        "order_id": order_id,
        "customer_id": random.choice(CUSTOMER_ID_POOL),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "order_date": _random_order_date(),
        "status": _random_status(),
    }


@app.get("/")
def root():
    return {"service": "mock-orders-api", "status": "ok"}


@app.get("/orders")
def get_orders(count: int = Query(default=20, ge=1, le=1000)):
    """
    Returns a freshly generated batch of `count` fake orders.
    order_id is a random large int per call (not sequential) so
    repeated runs don't collide, letting later "new records only"
    logic downstream work naturally.
    """
    orders = [
        _generate_order(order_id=fake.unique.random_int(min=100000, max=999999))
        for _ in range(count)
    ]
    fake.unique.clear()  # reset uniqueness pool for the next call
    return {"count": len(orders), "orders": orders}