*/
==================================================================================================================
Create Database and Schemas
==================================================================================================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'

Warning:
  Running this script will drop the entire ' DataWarehouse' database if it exists.
  All the data in the database will be permenantly deleted. Proceed with caution and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
ENF;
GO

-- Creating a Database
USE master;
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO
  
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
