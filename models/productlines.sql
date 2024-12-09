{{ config(
    materialized='incremental',
    unique_key='productLine'
) }}

WITH batch_data AS (
    SELECT
        etl_batch_date,
        etl_batch_no
    FROM etlmetadata.batch_control
),
transformed_data AS (
    SELECT
        s.productLine,
        s.create_timestamp as src_create_timestamp,
        s.update_timestamp as src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        b.etl_batch_no,
        b.etl_batch_date,
        ROW_NUMBER() OVER (ORDER BY productLine) AS dw_product_line_id
    FROM devstage.productlines s CROSS JOIN batch_data b
)

SELECT
    dw_product_line_id,
    productLine,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM transformed_data

