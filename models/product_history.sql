{{ config(materialized="incremental", unique_key=["dw_product_id", "MSRP","effective_from_date"]) }}

with
    batch_data as (select etl_batch_date, etl_batch_no from etlmetadata.batch_control),
    update_candidates as (
        select ph2.dw_product_id
        from {{ this }} ph2
        join {{ ref("products") }} p2 on ph2.dw_product_id = p2.dw_product_id
        where ph2.msrp != p2.msrp and ph2.dw_active_record_ind = 1
    ),
    updates as (
        select
            ph.dw_product_id,
            ph.msrp,
            ph.effective_from_date,
            0 as dw_active_record_ind,
            dateadd(day, -1, b.etl_batch_date) as effective_to_date,
            ph.dw_create_timestamp,
            current_timestamp as dw_update_timestamp,
            ph.create_etl_batch_date,
            ph.create_etl_batch_no,
            b.etl_batch_no as update_etl_batch_no,
            b.etl_batch_date as update_etl_batch_date
        from {{ this }} ph
        cross join batch_data b
        where
            ph.dw_product_id in (select dw_product_id from update_candidates)
            and ph.dw_active_record_ind = 1
    ),
    final_merged_data as (
        select
            ph.dw_product_id,
            ph.msrp,
            ph.effective_from_date,
            coalesce(u.dw_active_record_ind,ph.dw_active_record_ind) as dw_active_record_ind,
            coalesce(u.effective_to_date,ph.effective_to_date) as effective_to_date,
            ph.dw_create_timestamp,
            coalesce(u.dw_update_timestamp,ph.dw_update_timestamp) as dw_update_timestamp,
            ph.create_etl_batch_date,
            ph.create_etl_batch_no,
            coalesce(u.update_etl_batch_no,ph.update_etl_batch_no) as update_etl_batch_no,
            coalesce(u.update_etl_batch_date,ph.update_etl_batch_date) as update_etl_batch_date
        from {{this}} ph left join updates u on ph.dw_product_id=u.dw_product_id and ph.msrp =u.msrp and ph.dw_active_record_ind=1
    ),
    inserts as (
        select
            p.dw_product_id,
            p.msrp,
            b.etl_batch_date as effective_from_date,
            1 as dw_active_record_ind,
            NULL::DATE as effective_to_date,
            current_timestamp as dw_create_timestamp,
            NULL::DATE as dw_update_timestamp,
            b.etl_batch_date as create_etl_batch_date,
            b.etl_batch_no as create_etl_batch_no,
            null::INTEGER as update_etl_batch_no,
            NULL::DATE as update_etl_batch_date
        from {{ ref("products") }} p
        left join
            final_merged_data ph
            on p.dw_product_id = ph.dw_product_id
            and ph.dw_active_record_ind = 1
        cross join batch_data b
        where ph.dw_product_id is null
    ),
    final_data as (
        select *
        from final_merged_data
        union all
        select *
        from inserts
    )
select *
from final_data

