
      
        
            delete from "dev"."devdw"."monthly_customer_summary"
            using "monthly_customer_summary__dbt_tmp070729215291"
            where (
                
                    "monthly_customer_summary__dbt_tmp070729215291".start_of_the_month_date = "dev"."devdw"."monthly_customer_summary".start_of_the_month_date
                    and 
                
                    "monthly_customer_summary__dbt_tmp070729215291".dw_customer_id = "dev"."devdw"."monthly_customer_summary".dw_customer_id
                    
                
                
            );
        
    

    insert into "dev"."devdw"."monthly_customer_summary" ("start_of_the_month_date", "dw_customer_id", "order_count", "order_apd", "order_apm", "order_cost_amount", "cancelled_order_count", "cancelled_order_amount", "cancelled_order_apd", "cancelled_order_apm", "shipped_order_count", "shipped_order_amount", "shipped_order_apd", "shipped_order_apm", "payment_apd", "payment_apm", "payment_amount", "products_ordered_qty", "products_items_qty", "order_mrp_amount", "new_customer_apd", "new_customer_apm", "new_customer_paid_apd", "new_customer_paid_apm", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "start_of_the_month_date", "dw_customer_id", "order_count", "order_apd", "order_apm", "order_cost_amount", "cancelled_order_count", "cancelled_order_amount", "cancelled_order_apd", "cancelled_order_apm", "shipped_order_count", "shipped_order_amount", "shipped_order_apd", "shipped_order_apm", "payment_apd", "payment_apm", "payment_amount", "products_ordered_qty", "products_items_qty", "order_mrp_amount", "new_customer_apd", "new_customer_apm", "new_customer_paid_apd", "new_customer_paid_apm", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "monthly_customer_summary__dbt_tmp070729215291"
    )
  