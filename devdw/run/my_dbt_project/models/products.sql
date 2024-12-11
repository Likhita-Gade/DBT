
      
        
            delete from "dev"."devdw"."products"
            where (
                src_productCode) in (
                select (src_productCode)
                from "products__dbt_tmp074555759687"
            );

        
    

    insert into "dev"."devdw"."products" ("dw_product_id", "productscale", "quantityinstock", "buyprice", "msrp", "dw_product_line_id", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "src_productcode", "productname", "productline", "productvendor")
    (
        select "dw_product_id", "productscale", "quantityinstock", "buyprice", "msrp", "dw_product_line_id", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date", "src_productcode", "productname", "productline", "productvendor"
        from "products__dbt_tmp074555759687"
    )
  