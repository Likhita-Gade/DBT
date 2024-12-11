
      
        
            delete from "dev"."devdw"."orders"
            where (
                src_orderNumber) in (
                select (src_orderNumber)
                from "orders__dbt_tmp074659528474"
            );

        
    

    insert into "dev"."devdw"."orders" ("dw_order_id", "dw_customer_id", "src_ordernumber", "orderdate", "requireddate", "shippeddate", "src_customernumber", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "cancelleddate", "status")
    (
        select "dw_order_id", "dw_customer_id", "src_ordernumber", "orderdate", "requireddate", "shippeddate", "src_customernumber", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "cancelleddate", "status"
        from "orders__dbt_tmp074659528474"
    )
  