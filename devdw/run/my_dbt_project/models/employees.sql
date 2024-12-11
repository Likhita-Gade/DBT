
      
        
            delete from "dev"."devdw"."employees"
            where (
                employeeNumber) in (
                select (employeeNumber)
                from "employees__dbt_tmp074428098553"
            );

        
    

    insert into "dev"."devdw"."employees" ("dw_employee_id", "employeenumber", "lastname", "firstname", "extension", "email", "officecode", "reportsto", "jobtitle", "dw_office_id", "dw_reporting_employee_id", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date")
    (
        select "dw_employee_id", "employeenumber", "lastname", "firstname", "extension", "email", "officecode", "reportsto", "jobtitle", "dw_office_id", "dw_reporting_employee_id", "src_create_timestamp", "src_update_timestamp", "dw_create_timestamp", "dw_update_timestamp", "etl_batch_no", "etl_batch_date"
        from "employees__dbt_tmp074428098553"
    )
  