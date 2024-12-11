

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    existing_data as (
        select dw_office_id, officecode, dw_create_timestamp from "dev"."devdw"."offices"
    ),
    max_row_number as (
        select max(dw_office_id) as max_office_id
        from "dev"."devdw"."offices"
    )
select
    s.officecode,
    s.city,
    s.phone,
    s.addressline1,
    s.addressline2,
    s.state,
    s.country,
    s.postalcode,
    s.territory,
    s.create_timestamp as src_create_timestamp,
    s.update_timestamp as src_update_timestamp,
    case
        when ed.dw_create_timestamp is not null
        then ed.dw_create_timestamp
        else current_timestamp
    end as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date,
    case
        when ed.dw_office_id is not null
        then ed.dw_office_id
        else row_number() over() + coalesce(r.max_office_id,0)
    end as dw_office_id
from devstage.offices s
left join existing_data ed on s.officecode = ed.officecode
cross join batch_data b cross join max_row_number r