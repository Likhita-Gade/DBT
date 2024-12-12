
      
        
            delete from "dev"."devdw"."monthly_product_summary"
            using "monthly_product_summary__dbt_tmp100639229544"
            where (
                
                    "monthly_product_summary__dbt_tmp100639229544".start_of_the_month_date = "dev"."devdw"."monthly_product_summary".start_of_the_month_date
                    and 
                
                    "monthly_product_summary__dbt_tmp100639229544".dw_product_id = "dev"."devdw"."monthly_product_summary".dw_product_id
                    
                
                
            );
        
    

    insert into "dev"."devdw"."monthly_product_summary" ("start_of_the_month_date", "dw_product_id", "customer_apd", "customer_apm", "product_cost_amount", "product_mrp_amount", "cancelled_product_qty", "cancelled_cost_amount", "cancelled_mrp_amount", "cancelled_order_apd", "cancelled_order_apm", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "start_of_the_month_date", "dw_product_id", "customer_apd", "customer_apm", "product_cost_amount", "product_mrp_amount", "cancelled_product_qty", "cancelled_cost_amount", "cancelled_mrp_amount", "cancelled_order_apd", "cancelled_order_apm", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "monthly_product_summary__dbt_tmp100639229544"
    )
  