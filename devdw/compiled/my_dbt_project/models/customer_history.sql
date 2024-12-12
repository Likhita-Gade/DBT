

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    update_candidates as (
        select ph2.dw_customer_id
        from "dev"."devdw"."customer_history" ph2
        join "dev"."devdw"."customers" p2 on ph2.dw_customer_id = p2.dw_customer_id
        where ph2.creditlimit != p2.creditlimit and ph2.dw_active_record_ind = 1
    ),
    updates as (
        select
            ph.dw_customer_id,
            ph.creditlimit,
            ph.effective_from_date,
            0 as dw_active_record_ind,
            DATEADD(day, -1, b.etl_batch_date) as effective_to_date,
            ph.dw_create_timestamp,
            current_timestamp as dw_update_timestamp,
            ph.create_etl_batch_date,
            ph.create_etl_batch_no,
            b.etl_batch_no as update_etl_batch_no,
            b.etl_batch_date as update_etl_batch_date
        from "dev"."devdw"."customer_history" ph
        cross join batch_data b
        where
            ph.dw_customer_id in (select dw_customer_id from update_candidates)
            and ph.dw_active_record_ind = 1
    ),
    inserts as (
        select
            p.dw_customer_id,
            p.creditlimit,
            b.etl_batch_date as effective_from_date,
            1 as dw_active_record_ind,
            null as effective_to_date,
            current_timestamp as dw_create_timestamp,
            current_timestamp as dw_update_timestamp,
            b.etl_batch_date as create_etl_batch_date,
            b.etl_batch_no as create_etl_batch_no,
            null as update_etl_batch_no,
            null as update_etl_batch_date
        from "dev"."devdw"."customers" p
        left join
            "dev"."devdw"."customer_history" ph
            on p.dw_customer_id = ph.dw_customer_id
            and ph.dw_active_record_ind = 1
        cross join batch_data b
        where ph.dw_customer_id is null
    ),
    merged_data as (
        select *
        from updates
        union all
        select *
        from inserts
    )
select *
from merged_data