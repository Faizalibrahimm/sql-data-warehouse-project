/*
==============================
Create Database and Schemas
==============================

Script Purpose:
  This script creates a new database named 'datawarehouse' make sure there are no duplicate database names. 
  Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
  Ensure that there is no database with the same name as “datawarehouse.” If there is a database with
  the same name, you can change the database name to another name in the “CREATE DATABASE <other_name>” script.
  Run the scripts one by one in order from the top.
*/

-- Create the "datawarehouse" database 
CREATE DATABASE datawarehouse;

-- Create schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
