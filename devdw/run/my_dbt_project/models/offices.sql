
      
        
            delete from "dev"."devdw"."offices"
            where (
                officeCode) in (
                select (officeCode)
                from "offices__dbt_tmp074408800478"
            );

        
    

    insert into "dev"."devdw"."offices" ("dw_office_id", "officecode", "city", "phone", "addressline1", "addressline2", "state", "country", "postalcode", "territory", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "dw_office_id", "officecode", "city", "phone", "addressline1", "addressline2", "state", "country", "postalcode", "territory", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "offices__dbt_tmp074408800478"
    )
  