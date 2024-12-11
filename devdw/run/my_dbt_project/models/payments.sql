
      
        
            delete from "dev"."devdw"."payments"
            where (
                checkNumber) in (
                select (checkNumber)
                from "payments__dbt_tmp072745842297"
            );

        
    

    insert into "dev"."devdw"."payments" ("dw_payment_id", "dw_customer_id", "src_customernumber", "checknumber", "paymentdate", "amount", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "dw_payment_id", "dw_customer_id", "src_customernumber", "checknumber", "paymentdate", "amount", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "payments__dbt_tmp072745842297"
    )
  