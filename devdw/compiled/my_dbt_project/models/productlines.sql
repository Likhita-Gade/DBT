

WITH batch_data AS (
    SELECT
        etl_batch_date,
        etl_batch_no
    FROM etlmetadata.batch_control
),
existing_data as (
    select max(dw_product_line_id) as max_product_line_id 
    from "dev"."devdw"."productlines"
)
    SELECT
        s.productLine,
        s.create_timestamp as src_create_timestamp,
        s.update_timestamp as src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        b.etl_batch_no,
        b.etl_batch_date,
        row_number() over() + coalesce(ed.max_product_line_id,0) AS dw_product_line_id
    FROM devstage.productlines s CROSS JOIN batch_data b cross join existing_data ed