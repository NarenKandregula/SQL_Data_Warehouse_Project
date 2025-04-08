/*
=================================================================================
Quality Checks
=================================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwated spaces in string fields.
  - Data standardization and consistency.
  - Invalid data ranges and orders.
  - Data consistancy between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discripencies found during the checks.
=================================================================================
*/

/*
=================================================================================
crm_cust_info
=================================================================================
*/

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Solution:
SELECT *
FROM
	(SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1

-- Check for unwanted spaces
-- Expectation = No Results

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gnder
FROM bronze.crm_cust_info
WHERE cst_gnder != TRIM(cst_gnder)

-- Solution:
SELECT cst_id, cst_key, TRIM(cst_firstname) as cst_firstname, TRIM(cst_lastname) as cst_lastname,cst_material_status, cst_gnder, cst_create_date
FROM bronze.crm_cust_info

--Data Standardization & Consistency
SELECT DISTINCT cst_gnder
FROM bronze.crm_cust_info

SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info

--Solution
SELECT 
cst_id, 
cst_key, 
cst_firstname , 
cst_lastname, 
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S'  THEN 'Single'
	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_material_status, 
CASE WHEN UPPER(TRIM(cst_gnder)) = 'F'  THEN 'Female'
	WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
	ELSE 'n/a'
	END cst_gnder,
cst_create_date
FROM bronze.crm_cust_info

/*
=============================================================================================
SOLUTION ALL TOGETHER
*/

PRINT'>> Truncating Data: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
PRINT'>> Insering Data Into: silver.crm_cust_info'
INSERT INTO silver.crm_cust_info(
	cst_id, 
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gnder,
	cst_create_date
)
SELECT 
cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname, 
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S'  THEN 'Single'
	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_material_status, -- Normalize martial status values to readable format
CASE WHEN UPPER(TRIM(cst_gnder)) = 'F'  THEN 'Female'
	WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gnder, -- Normalize gender values to readable format
cst_create_date
FROM 
	(SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1 -- Select the most recent record per customer

/*
=================================================================================
crm_prd_info
=================================================================================
*/
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL 

--Deriving New columns from prd_key
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Check for unwanted spaces
-- Expectation = No Results

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLS or Negative Numbers
-- Expectation = No Results

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--SOlution
SELECT
	prd_id,
	prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--Solution
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
		CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--Solution
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info

/*
=============================================================================================
SOLUTION ALL TOGETHER
*/

PRINT'>> Truncating Data: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
PRINT'>> Insering Data Into: silver.crm_prd_info'
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         -- Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,   -- Map product line codes to descriptive values
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Calculate end date before the next start date
FROM bronze.crm_prd_info

/*
=================================================================================
crm_sales_details
=================================================================================
*/

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

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
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
WHERE sls_ord_num != TRIM(sls_ord_num)

--Checking if the product key in sales details table and product key in product information match
--Expectation: No Result
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
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

--Checking if the Customer ID in sales details table and Customer ID in Customer information match
--Expectation: No Result
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
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

--Chanking the Invalid dates
SELECT
sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0

--Solution
SELECT
NULLIF(sls_order_dt,0) sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

--Changing the data type of date column from integer to date
SELECT
NULLIF(sls_order_dt,0) sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101

--Solution
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

--Check for Invalid Date Orders
--Expectation: No Result
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check for data consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative.
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

--Solution
SELECT
sls_sales,
sls_quantity,
sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details


/*
=============================================================================================
SOLUTION ALL TOGETHER
*/

PRINT'>> Truncating Data: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
PRINT'>> Insering Data Into: silver.crm_sales_details'
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
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
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price -- Derive price if original value is invalid
FROM bronze.crm_sales_details

/*
=================================================================================
erp_cust_az12
=================================================================================
*/
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- Checking the cid key and clean it up to check for connectivity with other tables
--Solution
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- Identity OUT-of-Range Dates
SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--Solution
SELECT
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate
FROM bronze.erp_cust_az12

-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

--Solution
SELECT
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'FEMALE'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12
/*
=============================================================================================
SOLUTION ALL TOGETHER
*/

PRINT'>> Truncating Data: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
PRINT'>> Insering Data Into: silver.erp_cust_az12'
INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
	ELSE cid
END AS cid, -- Remove 'NAS' prefix if present
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate, -- Set future birthdates to NULL
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'FEMALE'
	ELSE 'n/a'
END AS gen -- Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12

/*
=================================================================================
erp_loc_a101
=================================================================================
*/
SELECT
cid,
cntry
FROM bronze.erp_loc_a101

-- Checking the cid key and clean it up to check for connectivity with other tables
--Solution
SELECT
REPLACE(cid, '-', '') cid
FROM bronze.erp_loc_a101

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

--SOlution
SELECT DISTINCT
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

/*
=============================================================================================
SOLUTION ALL TOGETHER
*/

PRINT'>> Truncating Data: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
PRINT'>> Insering Data Into: silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)

SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry --Normalize and Handle missing or blank country codes
FROM bronze.erp_loc_a101

/*
=================================================================================
erp_px_cat_g1v2
=================================================================================
*/
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

--Checking for unwanted spaces
SELECT
*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization and Consistancy
SELECT DISTINCT
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

/*
=============================================================================================
SOLUTION ALL TOGETHER

--No ERROR

*/

PRINT'>> Truncating Data: silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT'>> Insering Data Into: silver.erp_px_cat_g1v2'
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
FROM bronze.erp_px_cat_g1v2
