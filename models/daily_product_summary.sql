{{ config(materialized="incremental", unique_key=["summary_date","dw_product_id"]) }}

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    source_data as (
        select
            cast(o.orderdate as date) as summary_date,
            p.dw_product_id,
            1 as customer_apd,
            sum(od.priceeach * od.quantityordered) as product_cost_amount,
            sum(p.msrp * od.quantityordered) as product_mrp_amount,
            0 as cancelled_product_qty,
            0 as cancelled_cost_amount,
            0 as cancelled_mrp_amount,
            0 as cancelled_order_apd
        from {{ ref("products") }} p
        join {{ ref("orderdetails") }} od on p.dw_product_id = od.dw_product_id
        join {{ ref("orders") }} o on od.dw_order_id = o.dw_order_id
        cross join batch_data b
        where cast(o.orderdate as date) >= b.etl_batch_date
        group by cast(o.orderdate as date), p.dw_product_id

        union all

        select
            cast(o.cancelleddate as date) as summary_date,
            p.dw_product_id,
            0 as customer_apd,
            0 as product_cost_amount,
            0 as product_mrp_amount,
            count(p.dw_product_id) as cancelled_product_qty,
            sum(od.priceeach * od.quantityordered) as cancelled_cost_amount,
            sum(p.msrp * od.quantityordered) as cancelled_mrp_amount,
            1 as cancelled_order_apd
        from {{ ref("products") }} p
        join {{ ref("orderdetails") }} od on p.dw_product_id = od.dw_product_id
        join {{ ref("orders") }} o on od.dw_order_id = o.dw_order_id
        cross join batch_data b
        where cast(o.cancelleddate as date) >= b.etl_batch_date
        group by cast(o.cancelleddate as date), p.dw_product_id
    ),
    aggregated_data as (
        select
            s.summary_date,
            s.dw_product_id,
            max(s.customer_apd) as customer_apd,
            max(s.product_cost_amount) as product_cost_amount,
            max(s.product_mrp_amount) as product_mrp_amount,
            max(s.cancelled_product_qty) as cancelled_product_qty,
            max(s.cancelled_cost_amount) as cancelled_cost_amount,
            max(s.cancelled_mrp_amount) as cancelled_mrp_amount,
            max(s.cancelled_order_apd) as cancelled_order_apd
        from source_data s
        group by summary_date, dw_product_id
    )

select
    a.summary_date,
    a.dw_product_id,
    a.customer_apd,
    a.product_cost_amount,
    a.product_mrp_amount,
    a.cancelled_product_qty,
    a.cancelled_cost_amount,
    a.cancelled_mrp_amount,
    a.cancelled_order_apd,
    current_timestamp as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date
from aggregated_data a
cross join batch_data b

