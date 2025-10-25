/*
==================================================
Load Bronze Layer (Data Source -> Bronze)
==================================================
Script Purpose:
  This script loads data into the 'bronze' schema from external CSV files.
	Using 'COPY' command in PostgreSQL to load data from CSV Files to bronze tables.
	For the 'FROM' command, adjust the CSV data source path for each komputer.
	Run the scripts one by one in order from the top.
*/

COPY bronze.crm_cust_info(
	cst_id, 
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
FROM 'D:\Portofolio\dwh_project\datasets\source_crm\cust_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_prd_info(
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
FROM 'D:\Portofolio\dwh_project\datasets\source_crm\prd_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
FROM 'D:\Portofolio\dwh_project\datasets\source_crm\sales_details.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_loc_a101(cid, cntry)
FROM 'D:\Portofolio\dwh_project\datasets\source_erp\loc_a101.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_cust_az12(cid, bdate, gen)
FROM 'D:\Portofolio\dwh_project\datasets\source_erp\cust_az12.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_px_cat_g1v2(id, cat, subcat, maintenance)
FROM 'D:\Portofolio\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
DELIMITER ','
CSV HEADER;
