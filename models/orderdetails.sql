{{
    config(
        materialized="incremental",
        unique_key=["src_productcode","src_ordernumber"]
    )
}}

-- Fetch the latest batch metadata
with
    batch_control as (select etl_batch_no, etl_batch_date from etlmetadata.batch_control),
    existing_orderdetails as (
        select dw_orderdetail_id, src_ordernumber, src_productcode, dw_create_timestamp
        from {{ this }}
    ),
    max_row_number as (
        select max(dw_orderdetail_id) as max_orderdetail_id from {{ this }}
    )
select
    st.ordernumber as src_ordernumber,
    st.productcode as src_productcode,
    st.quantityordered,
    st.priceeach,
    st.orderlinenumber,
    st.create_timestamp as src_create_timestamp,
    st.update_timestamp as src_update_timestamp,
    case
        when dw.dw_create_timestamp is not null
        then dw.dw_create_timestamp
        else current_timestamp
    end as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    bc.etl_batch_no,
    bc.etl_batch_date,
    o.dw_order_id,
    p.dw_product_id,
    case
        when dw.dw_orderdetail_id is not null
        then dw.dw_orderdetail_id
        else row_number() over () + coalesce(r.max_orderdetail_id, 0)
    end as dw_orderdetail_id
from devstage.orderdetails st
left join
    existing_orderdetails dw
    on st.ordernumber = dw.src_ordernumber
    and st.productcode = dw.src_productcode
left join {{ ref("orders") }} o on st.ordernumber = o.src_ordernumber
left join {{ ref("products") }} p on st.productcode = p.src_productcode
cross join batch_control bc
cross join max_row_number r
