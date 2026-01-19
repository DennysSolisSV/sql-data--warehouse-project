/*
=====================================================================
Stored procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================

Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performds the folowing actions:
	- Truncate the bronze tables before loading data.
	- Uses the 'COPY' command to load data from csv files to bronze tables.
	
Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.
	
Usage Example:
	CALL bronze.load_bronze()
	
=====================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  start_time timestamp;
	end_time timestamp;
	start_time_whole_load timestamp;
	end_time_whole_load timestamp;
BEGIN
  
	start_time_whole_load = clock_timestamp();
	RAISE NOTICE '===================================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '===================================================';
	RAISE NOTICE '***************************************************';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '***************************************************';

	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	
	RAISE NOTICE 'Inserting data into: crm_cust_info';
	COPY bronze.crm_cust_info
	FROM '/var/lib/postgresql/imports/cust_info.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	
	RAISE NOTICE 'Inserting data into: crm_prd_info';
	COPY bronze.crm_prd_info
	FROM '/var/lib/postgresql/imports/prd_info.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: crm_sales_detail';
	TRUNCATE TABLE bronze.crm_sales_detail;
	
	RAISE NOTICE 'Inserting data into: crm_sales_detail';
	COPY bronze.crm_sales_detail
	FROM '/var/lib/postgresql/imports/sales_details.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	

	RAISE NOTICE '***************************************************';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '***************************************************';
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	RAISE NOTICE 'Inserting data into: erp_cust_az12';
	COPY bronze.erp_cust_az12
	FROM '/var/lib/postgresql/imports/CUST_AZ12.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	RAISE NOTICE 'Inserting data into: erp_loc_a101';
	COPY bronze.erp_loc_a101
	FROM '/var/lib/postgresql/imports/LOC_A101.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	RAISE NOTICE 'Inserting data into: erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2
	FROM '/var/lib/postgresql/imports/PX_CAT_G1V2.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	end_time_whole_load = clock_timestamp();
	RAISE NOTICE '===================================================';
	RAISE NOTICE 'Loading Bronze Layer is completed';
	RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time_whole_load - start_time_whole_load));
	RAISE NOTICE '===================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during data loading: %', SQLERRM;

END;
$$;
