
      
        
            delete from "dev"."devdw"."productlines"
            where (
                productLine) in (
                select (productLine)
                from "productlines__dbt_tmp072556510019"
            );

        
    

    insert into "dev"."devdw"."productlines" ("dw_product_line_id", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "productline")
    (
        select "dw_product_line_id", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "productline"
        from "productlines__dbt_tmp072556510019"
    )
  