
      
        
            delete from "dev"."devdw"."customer_history"
            using "customer_history__dbt_tmp095033227214"
            where (
                
                    "customer_history__dbt_tmp095033227214".dw_customer_id = "dev"."devdw"."customer_history".dw_customer_id
                    and 
                
                    "customer_history__dbt_tmp095033227214".creditlimit = "dev"."devdw"."customer_history".creditlimit
                    
                
                
            );
        
    

    insert into "dev"."devdw"."customer_history" ("dw_customer_id", "creditlimit", "effective_from_date", "effective_to_date", "dw_active_record_ind", "dw_create_timestamp", "dw_update_timestamp", "create_etl_batch_no", "create_etl_batch_date", "update_etl_batch_no", "update_etl_batch_date")
    (
        select "dw_customer_id", "creditlimit", "effective_from_date", "effective_to_date", "dw_active_record_ind", "dw_create_timestamp", "dw_update_timestamp", "create_etl_batch_no", "create_etl_batch_date", "update_etl_batch_no", "update_etl_batch_date"
        from "customer_history__dbt_tmp095033227214"
    )
  