-- Replaces the old Python aggregation from transform_load(): total
-- amount + order count, grouped by region and segment. Built on top
-- of the enriched_orders model above via ref(), so dbt understands
-- the dependency and always runs enriched_orders first.

{{ config(materialized='table') }}

select
    region,
    segment,
    sum(amount) as total_amount,
    count(*) as order_count
from {{ ref('enriched_orders') }}
group by region, segment