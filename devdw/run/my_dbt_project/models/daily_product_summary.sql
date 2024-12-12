
      
        
            delete from "dev"."devdw"."daily_product_summary"
            using "daily_product_summary__dbt_tmp095313021042"
            where (
                
                    "daily_product_summary__dbt_tmp095313021042".summary_date = "dev"."devdw"."daily_product_summary".summary_date
                    and 
                
                    "daily_product_summary__dbt_tmp095313021042".dw_product_id = "dev"."devdw"."daily_product_summary".dw_product_id
                    
                
                
            );
        
    

    insert into "dev"."devdw"."daily_product_summary" ("summary_date", "dw_product_id", "customer_apd", "product_cost_amount", "product_mrp_amount", "cancelled_product_qty", "cancelled_cost_amount", "cancelled_mrp_amount", "cancelled_order_apd", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "summary_date", "dw_product_id", "customer_apd", "product_cost_amount", "product_mrp_amount", "cancelled_product_qty", "cancelled_cost_amount", "cancelled_mrp_amount", "cancelled_order_apd", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "daily_product_summary__dbt_tmp095313021042"
    )
  