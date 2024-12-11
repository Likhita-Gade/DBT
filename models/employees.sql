{{ config(materialized="incremental", unique_key="employeeNumber") }}


with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    existing_data as (
        select dw_employee_id, employeenumber, dw_create_timestamp from {{ this }}
    ),
    max_row_number as (
        select max(dw_employee_id) as max_employee_id
        from {{this}}
    ),
    source_data as (
        select
            st.employeenumber,
            st.lastname,
            st.firstname,
            st.email,
            st.officecode,
            st.reportsto,
            st.jobtitle,
            ofc.dw_office_id,
            st.extension,
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
                when ed.dw_employee_id is not null
                then ed.dw_employee_id
                else row_number() over() + coalesce(r.max_employee_id,0)
            end as dw_employee_id
        from devstage.employees st
        join {{ ref("offices") }} ofc on st.officecode = ofc.officecode
        left join existing_data ed on st.employeenumber = ed.employeenumber
        cross join batch_data b cross join max_row_number r
    )
select
    s1.dw_employee_id,
    s1.employeenumber,
    s1.lastname,
    s1.firstname,
    s1.email,
    s1.officecode,
    s1.reportsto,
    s1.jobtitle,
    s1.dw_office_id,
    s2.dw_employee_id as dw_reporting_employee_id,
    s1.extension,
    s1.src_create_timestamp,
    s1.src_update_timestamp,
    s1.dw_create_timestamp,
    s1.dw_update_timestamp,
    s1.etl_batch_no,
    s1.etl_batch_date
from source_data s1
left join source_data s2 on s1.reportsto = s2.employeenumber
