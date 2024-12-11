
      
        
            delete from "dev"."devdw"."customers"
            where (
                src_customerNumber) in (
                select (src_customerNumber)
                from "customers__dbt_tmp074500194855"
            );

        
    

    insert into "dev"."devdw"."customers" ("dw_customer_id", "src_customernumber", "customername", "contactlastname", "contactfirstname", "phone", "addressline1", "addressline2", "city", "state", "postalcode", "country", "salesrepemployeenumber", "creditlimit", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "dw_customer_id", "src_customernumber", "customername", "contactlastname", "contactfirstname", "phone", "addressline1", "addressline2", "city", "state", "postalcode", "country", "salesrepemployeenumber", "creditlimit", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "customers__dbt_tmp074500194855"
    )
  