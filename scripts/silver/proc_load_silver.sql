/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time timestamp;
	end_time timestamp;
	start_time_whole_load timestamp;
	end_time_whole_load timestamp;
	v_rows   bigint;
BEGIN
  
	start_time_whole_load = clock_timestamp();
	RAISE NOTICE '===================================================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '===================================================';
	RAISE NOTICE '***************************************************';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '***************************************************';

	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	
	RAISE NOTICE 'Inserting data into: silver.crm_cust_info';

	
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
	 cst_id,
	 cst_key,
	 TRIM(cst_firstname) AS cst_firstname,
	 TRIM(cst_lastname) AS cst_lastname,
	 CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		  WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		  ELSE 'n/a' 
	 END AS cst_marital_status, -- Normalize marital status values to readable format
	 CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		  ELSE 'n/a' 
	 END AS cst_gndr, -- Normalize gender values to readable format
	 cst_create_date
	FROM (
		SELECT 
		 *,
		 ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		 FROM bronze.crm_cust_info 
	 )T WHERE flag_last = 1;

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	RAISE NOTICE 'Rows inserted into silver.crm_cust_info: ( % )', v_rows;
	 
 
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	
	RAISE NOTICE 'Inserting data into: silver.crm_prd_info';

	
	INSERT INTO silver.crm_prd_info (
		prd_id,
		prd_key,
		cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
		REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
		TRIM(prd_nm) AS prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a' 
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,  
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt  
	FROM bronze.crm_prd_info;

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	RAISE NOTICE 'Rows inserted into silver.crm_prd_info: ( % )', v_rows;


	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: silver.crm_sales_detail';
	TRUNCATE TABLE silver.crm_sales_detail;
	
	RAISE NOTICE 'Inserting data into: silver.crm_sales_detail';

	
	INSERT INTO silver.crm_sales_detail (
		sls_order_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT 
		sls_order_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 then NULL
			ELSE CAST(CAST(sls_order_dt AS TEXT) AS DATE) 
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 then NULL
			ELSE CAST(CAST(sls_ship_dt AS TEXT) AS DATE) 
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 then NULL
			ELSE CAST(CAST(sls_due_dt AS TEXT) AS DATE) 
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			  THEN sls_quantity * sls_price
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price < 0 THEN ABS(sls_price)
			WHEN sls_price IS NULL OR sls_price = 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		  ELSE sls_price
		END sls_price
	FROM bronze.crm_sales_detail;

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	RAISE NOTICE 'Rows inserted into silver.crm_sales_detail: ( % )', v_rows;

	
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	

	RAISE NOTICE '***************************************************';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '***************************************************';
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	
	RAISE NOTICE 'Inserting data into: silver.erp_cust_az12';

	
	INSERT INTO silver.erp_cust_az12(
		cid,
		b_date,
		gen
	)
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE WHEN b_date > CURRENT_DATE THEN NULL
			ELSE b_date
		END b_date,
		CASE WHEN UPPER(TRIM(gen)) = 'M' OR UPPER(TRIM(gen)) = 'MALE' THEN 'Male'
			WHEN UPPER(TRIM(gen))= 'F' OR UPPER(TRIM(gen))= 'FEMALE' THEN 'Female'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	RAISE NOTICE 'Rows inserted into silver.erp_cust_az12: ( % )', v_rows;

	
	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: silvererp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	
	RAISE NOTICE 'Inserting data into: silver.erp_loc_a101';
	

	INSERT INTO silver.erp_loc_a101
	(cid, cntry)
	SELECT 
		REPLACE(cid, '-', '') AS cid,
		CASE WHEN UPPER(TRIM(cntry)) = 'US' OR UPPER(TRIM(cntry)) = 'USA' THEN 'United States'
			WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		  ELSE TRIM(cntry)
		 END AS cntry
	FROM bronze.erp_loc_a101;

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	RAISE NOTICE 'Rows inserted into silver.erp_loc_a101: ( % )', v_rows;


	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	start_time = clock_timestamp();
	RAISE NOTICE '>>------------------------';
	RAISE NOTICE 'Truncating table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
	RAISE NOTICE 'Inserting data into: silver.erp_px_cat_g1v2';
	

	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	RAISE NOTICE 'Rows inserted into silver.erp_px_cat_g1v2: ( % )', v_rows;


	end_time = clock_timestamp();
	
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	end_time_whole_load = clock_timestamp();
	RAISE NOTICE '===================================================';
	RAISE NOTICE 'Loading Silver Layer is completed';
	RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time_whole_load - start_time_whole_load));
	RAISE NOTICE '===================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during data loading: %', SQLERRM;

END;
$$;
