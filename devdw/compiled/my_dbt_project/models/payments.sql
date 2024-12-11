


WITH batch_data AS (
    SELECT
        etl_batch_date,
        etl_batch_no
    FROM etlmetadata.batch_control
),
existing_data as (
    select max(dw_payment_id) as max_payment_id 
    FROM "dev"."devdw"."payments"
)
SELECT 
    c.dw_customer_id,
    st.customerNumber AS src_customerNumber,
    st.checkNumber,
    st.paymentDate,
    st.amount,
    st.create_timestamp AS src_create_timestamp,
    st.update_timestamp AS src_update_timestamp,
    CURRENT_TIMESTAMP AS dw_create_timestamp,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date,
    row_number() over() + coalesce(ed.max_payment_id,0) as dw_payment_id
FROM devstage.Payments st
 JOIN "dev"."devdw"."customers" c ON c.src_customerNumber = st.customerNumber 
 CROSS JOIN batch_data b cross join existing_data ed