/*
==================================================
Data Quality Check
==================================================
Script Purpose:
	This script is used to check the data quality of each table.
	- NULL or Duplicates data (especially primary table)
	- Data Standarization and consistency
	- Data Consistency between related fields
	- Invalid Date range and order
	- Invalid Data type
	- Unwanted Space in string fields

Usage Notes:
	Run the scripts one by one in order and understand each data cleansing and correction.
*/


-- Quality data table bronze 'crm_cust_info'
-- Check for NULL or Duplicates in PK
-- Expectation: No result
SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
ORDER BY cst_id;

SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;	--example

-- Check for unwanted spaces
-- Expectation: No result
SELECT 
	cst_firstname 	
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT 
	cst_lastname 	
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT 
	cst_gndr	
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT 
	cst_key 
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standarization & Consistency
-- Expectation: Consistent
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Final Check in Silver Schema 'crm_cust_info'
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT 
	cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

SELECT 
	cst_firstname  
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


-- Quality data table bronze 'crm_prd_info'
-- Check for NULL or Duplicates in PK
-- Expectation: No result
SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS sls_prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2;
SELECT sls_prd_key FROM bronze.crm_sales_details;

-- Check for unwanted spaces
-- Expectation: No result
SELECT 
	prd_nm  	
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULL or Negative Numbers
SELECT 
	prd_cost  	
FROM bronze.crm_prd_info
WHERE prd_cost < 0  OR prd_cost IS NULL;

-- Data Standarization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 days') AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509', 'AC-HE-HL-U509-R');

-- Final Check in Silver Schema 'crm_prd_info'
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT 
	prd_nm  	
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT 
	prd_cost  	
FROM silver.crm_prd_info
WHERE prd_cost < 0  OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- Quality data table bronze 'crm_sales_details'
-- Check for NULL or Duplicates in PK
-- Expectation: No result
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for Invalid Dates
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8;

SELECT 
	sls_order_dt,
	LENGTH(CAST(sls_order_dt AS TEXT))
FROM bronze.crm_sales_details;

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check data consistency: Between sales, quantity, and price
-- sales = quantity * price
-- Values must not be NULL, zero, or negative (-)
SELECT DISTINCT 
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

-- Final Check in Silver Schema 'crm_sales_details'
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT DISTINCT 
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;


-- Quality data table bronze 'erp_cust_az12'
SELECT 
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		 ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12;

-- Identify Out of Range Dates
SELECT DISTINCT 
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;

-- Data Standarization & Consistency
SELECT DISTINCT 
	gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- Final Check in Silver Schema 'erp_cust_az12'
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;


-- Quality data table bronze 'erp_loc_a101'
-- Data Standarization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT 
	cntry AS cntry_old,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

-- Final Check in Silver Schema 'erp_loc_a101'
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;


-- Quality data table bronze 'erp_px_cat_g1v2'
-- Check for Unwanted Space
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standarization & Consistency
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;
