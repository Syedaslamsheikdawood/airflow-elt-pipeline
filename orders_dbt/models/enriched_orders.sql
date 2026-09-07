-- Replaces the old Python join logic from transform_load(): one row
-- per order, with customer info attached. Materialized as a table
-- (not a view) since downstream consumers, including
-- revenue_by_region below, read from it repeatedly.

{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.region,
    c.segment,
    o.amount,
    o.order_date,
    o.status
from {{ source('airflow_pipeline', 'raw_orders') }} as o
inner join {{ source('airflow_pipeline', 'raw_customers') }} as c
    on o.customer_id = c.customer_id