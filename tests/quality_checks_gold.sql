/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--Checking for duplicate data
--Expectation: No Results

SELECT cst_id, COUNT(*) 
FROM (
	SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_material_status,
		ci.cst_gnder,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci 

	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid

	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
)t 
GROUP BY cst_id
HAVING COUNT(*) > 1

-- Data Integration(Gender)
SELECT DISTINCT
	ci.cst_gnder,
	ca.gen
FROM silver.crm_cust_info ci 

LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid

LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

ORDER BY 1,2

--Solution
SELECT DISTINCT
	ci.cst_gnder,
	ca.gen,
	CASE WHEN  ci.cst_gnder != 'n/a' THEN ci.cst_gnder -- CRM is the Master for gender Info
		ELSE COALESCE (ca.gen, 'n/a')
	END AS new_gen

 FROM silver.crm_cust_info ci 

LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid

LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

ORDER BY 1,2

/*
=============================================================================================
 - SOLUTION ALL TOGETHER with 
 - Renaming Headers 
 - Re-ordering
 - Creating Surrogate Key for the Table
*/

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS Customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_material_status AS martial_status,
	CASE WHEN  ci.cst_gnder != 'n/a' THEN ci.cst_gnder -- CRM is the Master for gender Info
		ELSE COALESCE (ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	
FROM silver.crm_cust_info ci 

LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid

LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid


-- ==============================================================================================

-- To select the currenty information(prd_end_dt = NULL) and checking the uniqness of product key (Expectation: No RESULT)
SELECT prd_key, COUNT(*)
FROM(
	SELECT 
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost, 
		pn.prd_line,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info pn

	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id

	WHERE pn.prd_end_dt IS NULL -- Filter out all historical data
)t GROUP BY prd_key
HAVING COUNT(*) > 1

/*
=============================================================================================
 - SOLUTION ALL TOGETHER with 
 - Renaming Headers 
 - Re-ordering
 - Creating Surrogate Key for the Table
*/

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost, 
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn

LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id

WHERE pn.prd_end_dt IS NULL -- Filter out all historical data

-- ==============================================================================================

-- BUILDING FACT
-- Use the dimension's surrogate keys instead of ID's to easily connect facts with dimensions
-- Renaming Headers 
-- Re-ordering

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price,
	sd.dwh_create_date
FROM silver.crm_sales_details sd

LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number

LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

-- QUALITY CHECK

SELECT * FROM gold.dim_customers
SELECT * FROM gold.dim_products
SELECT * FROM gold.fact_sales

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key

LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
