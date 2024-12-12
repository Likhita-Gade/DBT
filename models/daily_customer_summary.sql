{{ config(materialized="incremental", unique_key=["summary_date","dw_customer_id"]) }}

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    base_data as (
        select
            cast(src_create_timestamp as date) as summary_date,
            dw_customer_id,
            0 as order_count,
            0 as order_apd,
            0 as order_cost_amount,
            0 as cancelled_order_count,
            0 as cancelled_order_amount,
            0 as cancelled_order_apd,
            0 as shipped_order_count,
            0 as shipped_order_amount,
            0 as shipped_order_apd,
            0 as payment_apd,
            0 as payment_amount,
            0 as products_ordered_qty,
            0 as products_items_qty,
            0 as order_mrp_amount,
            1 as new_customer_apd,
            0 as new_customer_paid_apd
        from {{ ref("customers") }}
        cross join batch_data b
        where cast(src_create_timestamp as date) >= b.etl_batch_date
        group by cast(src_create_timestamp as date),
            dw_customer_id

        union all

        select
            cast(o.cancelleddate as date) as summary_date,
            o.dw_customer_id,
            0 as order_count,
            0 as order_apd,
            0 as order_cost_amount,
            count(distinct o.dw_order_id) as cancelled_order_count,
            sum(od.priceeach * od.quantityordered) as cancelled_order_amount,
            1 as cancelled_order_apd,
            0 as shipped_order_count,
            0 as shipped_order_amount,
            0 as shipped_order_apd,
            0 as payment_apd,
            0 as payment_amount,
            0 as products_ordered_qty,
            0 as products_items_qty,
            0 as order_mrp_amount,
            0 as new_customer_apd,
            0 as new_customer_paid_apd
        from {{ ref("orders") }} o
        join {{ ref("orderdetails") }} od on o.dw_order_id = od.dw_order_id
        cross join batch_data b
        where
            cast(o.cancelleddate as date) >= b.etl_batch_date
            and o.status like 'Cancelled'
        group by cast(o.cancelleddate as date),
            o.dw_customer_id

        union all

        select
            cast(o.shippeddate as date) as summary_date,
            o.dw_customer_id,
            0 as order_count,
            0 as order_apd,
            0 as order_cost_amount,
            0 as cancelled_order_count,
            0 as cancelled_order_amount,
            0 as cancelled_order_apd,
            count(distinct o.dw_order_id) as shipped_order_count,
            sum(od.priceeach * od.quantityordered) as shipped_order_amount,
            1 as shipped_order_apd,
            0 as payment_apd,
            0 as payment_amount,
            0 as products_ordered_qty,
            0 as products_items_qty,
            0 as order_mrp_amount,
            0 as new_customer_apd,
            0 as new_customer_paid_apd
        from {{ ref("orders") }} o
        join {{ ref("orderdetails") }} od on o.dw_order_id = od.dw_order_id
        cross join batch_data b
        where
            cast(o.shippeddate as date) >= b.etl_batch_date and o.status like 'Shipped'
        group by cast(o.shippeddate as date),
            o.dw_customer_id

        union all

        select
            cast(o.orderdate as date) as summary_date,
            o.dw_customer_id,
            count(distinct o.dw_order_id) as order_count,
            1 as order_apd,
            sum(p.buyprice * od.quantityordered) as order_cost_amount,
            0 as cancelled_order_count,
            0 as cancelled_order_amount,
            0 as cancelled_order_apd,
            0 as shipped_order_count,
            0 as shipped_order_amount,
            0 as shipped_order_apd,
            0 as payment_apd,
            0 as payment_amount,
            count(distinct p.dw_product_id) as products_ordered_qty,
            sum(od.quantityordered) as products_items_qty,
            sum(p.msrp * od.quantityordered) as order_mrp_amount,
            0 as new_customer_apd,
            0 as new_customer_paid_apd
        from {{ ref("orders") }} o
        join {{ ref("orderdetails") }} od on o.dw_order_id = od.dw_order_id
        join {{ ref("products") }} p on p.dw_product_id = od.dw_product_id
        cross join batch_data b
        where cast(o.orderdate as date) >= b.etl_batch_date
        group by cast(o.orderdate as date),
            o.dw_customer_id

        union all

        select
            cast(paymentdate as date) as summary_date,
            dw_customer_id,
            0 as order_count,
            0 as order_apd,
            0 as order_cost_amount,
            0 as cancelled_order_count,
            0 as cancelled_order_amount,
            0 as cancelled_order_apd,
            0 as shipped_order_count,
            0 as shipped_order_amount,
            0 as shipped_order_apd,
            1 as payment_apd,
            sum(amount) as payment_amount,
            0 as products_ordered_qty,
            0 as products_items_qty,
            0 as order_mrp_amount,
            0 as new_customer_apd,
            0 as new_customer_paid_apd
        from {{ ref("payments") }}
        cross join batch_data b
        where cast(paymentdate as date) >= b.etl_batch_date
        group by cast(paymentdate as date),
            dw_customer_id
    ),

    aggregated_data as (
        select
            summary_date,
            dw_customer_id,
            max(order_count) as order_count,
            max(order_apd) as order_apd,
            max(order_cost_amount) as order_cost_amount,
            max(cancelled_order_count) as cancelled_order_count,
            max(cancelled_order_amount) as cancelled_order_amount,
            max(cancelled_order_apd) as cancelled_order_apd,
            max(shipped_order_count) as shipped_order_count,
            max(shipped_order_amount) as shipped_order_amount,
            max(shipped_order_apd) as shipped_order_apd,
            max(payment_apd) as payment_apd,
            max(payment_amount) as payment_amount,
            max(products_ordered_qty) as products_ordered_qty,
            max(products_items_qty) as products_items_qty,
            max(order_mrp_amount) as order_mrp_amount,
            max(new_customer_apd) as new_customer_apd,
            max(new_customer_paid_apd) as new_customer_paid_apd
        from base_data
        group by summary_date, dw_customer_id
    )

select
    a.summary_date,
    a.dw_customer_id,
    a.order_count,
    a.order_apd,
    a.order_cost_amount,
    a.cancelled_order_count,
    a.cancelled_order_amount,
    a.cancelled_order_apd,
    a.shipped_order_count,
    a.shipped_order_amount,
    a.shipped_order_apd,
    a.payment_apd,
    a.payment_amount,
    a.products_ordered_qty,
    a.products_items_qty,
    a.order_mrp_amount,
    a.new_customer_apd,
    a.new_customer_paid_apd,
    current_timestamp as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date
from aggregated_data a
cross join batch_data b

