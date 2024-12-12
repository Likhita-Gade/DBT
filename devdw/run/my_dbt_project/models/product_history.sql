
      
        
            delete from "dev"."devdw"."product_history"
            using "product_history__dbt_tmp095146282067"
            where (
                
                    "product_history__dbt_tmp095146282067".dw_product_id = "dev"."devdw"."product_history".dw_product_id
                    and 
                
                    "product_history__dbt_tmp095146282067".MSRP = "dev"."devdw"."product_history".MSRP
                    
                
                
            );
        
    

    insert into "dev"."devdw"."product_history" ("dw_product_id", "msrp", "effective_from_date", "effective_to_date", "dw_active_record_ind", "dw_create_timestamp", "dw_update_timestamp", "create_etl_batch_no", "create_etl_batch_date", "update_etl_batch_no", "update_etl_batch_date")
    (
        select "dw_product_id", "msrp", "effective_from_date", "effective_to_date", "dw_active_record_ind", "dw_create_timestamp", "dw_update_timestamp", "create_etl_batch_no", "create_etl_batch_date", "update_etl_batch_no", "update_etl_batch_date"
        from "product_history__dbt_tmp095146282067"
    )
  