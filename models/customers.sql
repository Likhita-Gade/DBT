{{ config(materialized="incremental", unique_key="src_customerNumber") }}


with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    existing_data as (
        select dw_customer_id, src_customernumber, dw_create_timestamp from {{ this }}
    ),
    max_row_number as (
        select max(dw_customer_id) as max_customer_id
        from {{this}}
    )
select
    st.customernumber as src_customernumber,
    st.customername,
    st.contactlastname,
    st.contactfirstname,
    st.phone,
    st.addressline1,
    st.addressline2,
    st.city,
    st.state,
    st.postalcode,
    st.country,
    st.salesrepemployeenumber,
    st.creditlimit,
    st.create_timestamp as src_create_timestamp,
    st.update_timestamp as src_update_timestamp,
    em.dw_employee_id as dw_reporting_employee_id,
    case
        when ed.dw_create_timestamp is not null
        then ed.dw_create_timestamp
        else current_timestamp
    end as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date,
    case
        when ed.dw_customer_id is not null
        then ed.dw_customer_id
        else
            row_number() over() + coalesce(r.max_customer_id,0)
    end as dw_customer_id
from devstage.customers st
left join {{ref("employees")}} em on st.salesrepemployeenumber = em.employeenumber
left join existing_data ed on st.customerNumber=ed.src_customerNumber
cross join batch_data b cross join max_row_number r
