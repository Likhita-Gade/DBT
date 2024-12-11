

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    existing_data as (
        select dw_product_id, src_productcode, dw_create_timestamp from "dev"."devdw"."products"
    ),
    max_row_number as (
        select max(dw_product_id) as max_product_id
        from "dev"."devdw"."products"
    )
select
    st.productcode as src_productcode,
    st.productname,
    st.productline,
    pl.dw_product_line_id,
    st.productscale,
    st.productvendor,
    st.quantityinstock,
    st.buyprice,
    st.msrp,
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
        when ed.dw_product_id is not null
        then ed.dw_product_id
        else row_number() over() + coalesce(r.max_product_id,0)
    end as dw_product_id
from devstage.products as st
join "dev"."devdw"."productlines" as pl on st.productline = pl.productline
left join existing_data ed on ed.src_productcode = st.productcode
cross join batch_data b cross join max_row_number r