

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    aggergated_data as (
        select
            date_trunc('month', summary_date) as start_of_the_month_date,
            dw_customer_id,
            sum(order_count) as order_count,
            max(order_apd) as order_apd,
            max(order_apd) as order_apm,
            sum(order_cost_amount) as order_cost_amount,
            sum(cancelled_order_count) as cancelled_order_count,
            sum(cancelled_order_amount) as cancelled_order_amount,
            max(cancelled_order_apd) as cancelled_order_apd,
            max(cancelled_order_apd) as cancelled_order_apm,
            sum(shipped_order_count) as shipped_order_count,
            sum(shipped_order_amount) as shipped_order_amount,
            max(shipped_order_apd) as shipped_order_apd,
            max(shipped_order_apd) as shipped_order_apm,
            max(payment_apd) as payment_apd,
            max(payment_apd) as payment_apm,
            sum(payment_amount) as payment_amount,
            sum(products_ordered_qty) as products_ordered_qty,
            sum(products_items_qty) as products_items_qty,
            sum(order_mrp_amount) as order_mrp_amount,
            max(new_customer_apd) as new_customer_apd,
            max(new_customer_apd) as new_customer_apm,
            0 as new_customer_paid_apd,
            0 as new_customer_paid_apm
        from "dev"."devdw"."daily_customer_summary"
        cross join batch_data b
        where date_trunc('month', summary_date) >= date_trunc('month', b.etl_batch_date)
        group by date_trunc('month', summary_date), dw_customer_id
    )

select
    a.start_of_the_month_date,
    a.dw_customer_id,
    a.order_count,
    a.order_apd,
    a.order_apm,
    a.order_cost_amount,
    a.cancelled_order_count,
    a.cancelled_order_amount,
    a.cancelled_order_apd,
    a.cancelled_order_apm,
    a.shipped_order_count,
    a.shipped_order_amount,
    a.shipped_order_apd,
    a.shipped_order_apm,
    a.payment_apd,
    a.payment_apm,
    a.payment_amount,
    a.products_ordered_qty,
    a.products_items_qty,
    a.order_mrp_amount,
    a.new_customer_apd,
    a.new_customer_apm,
    a.new_customer_paid_apd,
    a.new_customer_paid_apm,
    current_timestamp as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date
from aggergated_data a
cross join batch_data b