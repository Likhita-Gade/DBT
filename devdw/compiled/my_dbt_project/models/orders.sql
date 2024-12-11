

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    existing_data as (
        select dw_order_id, src_orderNumber, dw_create_timestamp from "dev"."devdw"."orders"
    ),
    max_row_number as (
        select max(dw_order_id) as max_order_id
        from "dev"."devdw"."orders"
    )
select
    st.ordernumber as src_ordernumber,
    st.orderdate,
    st.requireddate,
    st.shippeddate,
    st.status,
    c.dw_customer_id,
    st.customernumber as src_customernumber,
    st.cancelleddate,
    st.create_timestamp as src_create_timestamp,
    st.update_timestamp as src_update_timestamp,
    case
        when ed.dw_create_timestamp is not null
        then ed.dw_create_timestamp
        else current_timestamp
    end as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date,
    case
        when ed.dw_order_id is not null
        then ed.dw_order_id
        else row_number() over() + coalesce(r.max_order_id,0)
    end as dw_order_id
from devstage.orders st
join "dev"."devdw"."customers" c on st.customernumber = c.src_customernumber
left join existing_data ed on ed.src_ordernumber = st.ordernumber
cross join batch_data b cross join max_row_number r