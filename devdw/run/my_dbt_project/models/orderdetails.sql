
      
        
            delete from "dev"."devdw"."orderdetails"
            using "orderdetails__dbt_tmp093729259725"
            where (
                
                    "orderdetails__dbt_tmp093729259725".src_productcode = "dev"."devdw"."orderdetails".src_productcode
                    and 
                
                    "orderdetails__dbt_tmp093729259725".src_ordernumber = "dev"."devdw"."orderdetails".src_ordernumber
                    
                
                
            );
        
    

    insert into "dev"."devdw"."orderdetails" ("dw_orderdetail_id", "dw_order_id", "dw_product_id", "src_ordernumber", "quantityordered", "priceeach", "orderlinenumber", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "src_productcode")
    (
        select "dw_orderdetail_id", "dw_order_id", "dw_product_id", "src_ordernumber", "quantityordered", "priceeach", "orderlinenumber", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "src_productcode"
        from "orderdetails__dbt_tmp093729259725"
    )
  