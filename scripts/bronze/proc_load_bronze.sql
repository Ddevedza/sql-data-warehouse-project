/* 

=====================================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze)

=====================================================================

Creating batch loading for bronze data. In this query we will define and bulk in each table from each csv in a controlled manner.
*/

USE DataWarehouse;
GO

-- Creating procedure for bronze batch data loading
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME,@end_batch_time DATETIME; -- defining time variables so we can check each separate execution, along with total batch execution
    BEGIN TRY
    SET @start_batch_time = GETDATE(); -- batch execution start
            PRINT '======================================================='; -- prints for nice viewing
            PRINT 'Loading Bronze Layer';
            PRINT '=======================================================';

            PRINT '-------------------------------------------------------';
            PRINT 'Loading CRM Tables';
            PRINT '-------------------------------------------------------';

            SET @start_time = GETDATE(); -- table exec measuring
            PRINT '>> Truncating Table: bronze.crm_cust_info';
            TRUNCATE TABLE bronze.crm_cust_info; -- truncating so tables are not duplicated once it is runned

            PRINT '>> Bulk inserting: bronze.crm_cust_info';
            BULK INSERT bronze.crm_cust_info -- bulk insert per table
            FROM 'C:\Users\Dusan\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' -- source
            WITH (
                FIRSTROW = 2, --where the bulk starts (first row is header)
                FIELDTERMINATOR = ',', -- what is separating each column data
                ROWTERMINATOR = '\n', -- it separates row data
                CODEPAGE = '65001', -- UTF 8
                TABLOCK -- it locks in whole table while loading in - good with bulk
            );

            SET @end_time = GETDATE(); -- catching end time
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'; -- creating time difference between start and end as NVARCHAR in seconds

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_prd_info';
            TRUNCATE TABLE bronze.crm_prd_info;

            PRINT '>> Bulk inserting: bronze.crm_prd_info';
            BULK INSERT bronze.crm_prd_info
            FROM 'C:\Users\Dusan\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                CODEPAGE = '65001',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_sales_details';
            TRUNCATE TABLE bronze.crm_sales_details;

            PRINT '>> Bulk inserting: bronze.crm_sales_details';
            BULK INSERT bronze.crm_sales_details
            FROM 'C:\Users\Dusan\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                CODEPAGE = '65001',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

            PRINT '-------------------------------------------------------';
            PRINT 'Loading ERP Tables';
            PRINT '-------------------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.erp_cust_az12';
            TRUNCATE TABLE bronze.erp_cust_az12;

            PRINT '>> Bulk inserting: bronze.erp_cust_az12';
            BULK INSERT bronze.erp_cust_az12
            FROM 'C:\Users\Dusan\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                CODEPAGE = '65001',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.erp_loc_a101';
            TRUNCATE TABLE bronze.erp_loc_a101;

            PRINT '>> Bulk inserting: bronze.erp_loc_a101';
            BULK INSERT bronze.erp_loc_a101
            FROM 'C:\Users\Dusan\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                CODEPAGE = '65001',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;

            PRINT '>> Bulk inserting: bronze.erp_px_cat_g1v2';
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'C:\Users\Dusan\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                CODEPAGE = '65001',
                TABLOCK
            );
            SET @end_time = GETDATE();
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds';

            PRINT '=======================================================';
            PRINT 'Bronze load finished successfully.';
            PRINT '=======================================================';
            SET @end_batch_time = GETDATE();
            PRINT'>> Load Duration: '+ CAST(DATEDIFF(second,@start_batch_time,@end_batch_time) AS NVARCHAR)+' seconds';
        END TRY
        BEGIN CATCH -- Error catching
            PRINT '=======================================================';
            PRINT 'Bronze load FAILED.';
            PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS varchar(20)); -- which number error was made
            PRINT 'Error message: ' + ERROR_MESSAGE(); -- printing error message
            PRINT 'Error line: ' + CAST(ERROR_LINE() AS varchar(20)); -- printing at which line it occured
            PRINT '=======================================================';
        END CATCH
END
GO

EXEC bronze.load_bronze; -- exec the whole load
GO
