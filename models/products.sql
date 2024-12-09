{{ config(
    materialized='incremental',
    unique_key='src_productCode'
) }}

WITH source_data AS (
    SELECT
        productCode,
        productName,
        productLine,
        productScale,
        productVendor,
        quantityInStock,
        buyPrice,
        MSRP,
        create_timestamp,
        update_timestamp
    FROM devstage.Products AS st
),
batch_data AS (
    SELECT
        etl_batch_date,
        etl_batch_no
    FROM etlmetadata.batch_control
),
transformed_data AS (
    SELECT
        st.productCode AS src_productCode,
        st.productName,
        st.productLine,
        pl.dw_product_line_id,
        st.productScale,
        st.productVendor,
        st.quantityInStock,
        st.buyPrice,
        st.MSRP,
        st.create_timestamp AS src_create_timestamp,
        st.update_timestamp AS src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        b.etl_batch_no,
        b.etl_batch_date,
        ROW_NUMBER() OVER (ORDER BY st.productCode) AS dw_product_id
    FROM source_data AS st
    JOIN devdw.ProductLines AS pl 
        ON st.productLine = pl.productLine
    CROSS JOIN batch_data b
)
SELECT
    dw_product_id,
    src_productCode,
    productName,
    productLine,
    dw_product_line_id,
    productScale,
    productVendor,
    quantityInStock,
    buyPrice,
    MSRP,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM transformed_data

