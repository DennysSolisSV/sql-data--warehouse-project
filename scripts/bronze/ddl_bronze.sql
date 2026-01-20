/*
=====================================================================
DDL Script: Create Bronze Tables
=====================================================================

Script Purpose:
	This script create tables in the 'bronze' schema, dropping existing tables
	if they already exist.
	
	Run this script to re-define the DDL structure of 'bronze' tables
	
=====================================================================
*/


DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key TEXT,
    prd_nm TEXT,
    prd_cost TEXT,
    prd_line TEXT,
    prd_start_dt DATE,
    prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_detail;
CREATE TABLE bronze.crm_sales_detail (
    sls_order_num TEXT,
    sls_prd_key TEXT,
    sls_cust_id  INT,
    sls_order_dt TEXT,
    sls_ship_dt TEXT,
    sls_due_dt TEXT,
    sls_sales TEXT,
    sls_quantity INT,
    sls_price TEXT
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid TEXT,
    b_date DATE,
    gen TEXT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid TEXT,
    cntry TEXT
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
    id TEXT,
    cat TEXT,
    subcat TEXT,
    maintenance TEXT
);
