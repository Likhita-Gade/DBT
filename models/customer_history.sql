{{ config(materialized="incremental", unique_key=["dw_customer_id","creditlimit","effective_from_date"])}}

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    update_candidates as (
        select ph2.dw_customer_id
        from {{ this }} ph2
        join {{ ref("customers") }} p2 on ph2.dw_customer_id = p2.dw_customer_id
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
        from {{ this }} ph
        cross join batch_data b
        where
            ph.dw_customer_id in (select dw_customer_id from update_candidates)
            and ph.dw_active_record_ind = 1
    ),
    final_merged_data as (
        select
            ph.dw_customer_id,
            ph.creditlimit,
            ph.effective_from_date,
            coalesce(u.dw_active_record_ind,ph.dw_active_record_ind) as dw_active_record_ind,
            coalesce(u.effective_to_date,ph.effective_to_date) as effective_to_date,
            ph.dw_create_timestamp,
            coalesce(u.dw_update_timestamp,ph.dw_update_timestamp) as dw_update_timestamp,
            ph.create_etl_batch_date,
            ph.create_etl_batch_no,
            coalesce(u.update_etl_batch_no,ph.update_etl_batch_no) as update_etl_batch_no,
            coalesce(u.update_etl_batch_date,ph.update_etl_batch_date) as update_etl_batch_date
        from {{this}} ph left join updates u on ph.dw_customer_id=u.dw_customer_id and ph.creditlimit =u.creditlimit and ph.dw_active_record_ind=1
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
        from {{ ref("customers") }} p
        left join
            final_merged_data ph
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

