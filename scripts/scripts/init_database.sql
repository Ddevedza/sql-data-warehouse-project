-- Create Database 'DataWarehouse'

USE master; 

IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse') -- in case Datawarehouse exists
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE; -- we drop it
	DROP DATABASE DataWarehouse; 
END
GO

CREATE DATABASE DataWarehouse; -- then create it

GO

USE DataWarehouse;

GO --Separates the one by one run of each line

-- Creating separate folders for each medalion --

CREATE SCHEMA bronze; 

GO

CREATE SCHEMA silver;

GO

CREATE SCHEMA gold;
