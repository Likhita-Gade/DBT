{{ config(materialized="incremental", unique_key=["start_of_the_month_date","dw_product_id"]) }}

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    aggregated_data as (
        select
            date_trunc('month', summary_date) as start_of_the_month_date,
            dw_product_id,
            max(customer_apd) as customer_apd,
            max(customer_apd) as customer_apm,
            sum(product_cost_amount) as product_cost_amount,
            sum(product_mrp_amount) as product_mrp_amount,
            sum(cancelled_product_qty) as cancelled_product_qty,
            sum(cancelled_cost_amount) as cancelled_cost_amount,
            sum(cancelled_mrp_amount) as cancelled_mrp_amount,
            max(cancelled_order_apd) as cancelled_order_apd,
            max(cancelled_order_apd) as cancelled_order_apm
        from {{ ref("daily_product_summary") }}
        cross join batch_data b
        where date_trunc('month', summary_date) >= date_trunc('month', b.etl_batch_date)
        group by date_trunc('month', summary_date), dw_product_id
    )

select
    a.start_of_the_month_date,
    a.dw_product_id,
    a.customer_apd,
    a.customer_apm,
    a.product_cost_amount,
    a.product_mrp_amount,
    a.cancelled_product_qty,
    a.cancelled_cost_amount,
    a.cancelled_mrp_amount,
    a.cancelled_order_apd,
    a.cancelled_order_apm,
    current_timestamp as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date
from aggregated_data a
cross join batch_data b

