/*
===============================================================================
Stored Procedure: Caricamento Layer Bronze (Sorgente -> Bronze)
===============================================================================
Scopo dello Script:
    Questa stored procedure carica i dati nello schema 'bronze' da file CSV esterni.
    Esegue le seguenti operazioni:
    - Svuota le tabelle bronze prima del caricamento dei dati.
    - Utilizza il comando `BULK INSERT` per caricare i dati dai file CSV nelle tabelle bronze.

Parametri:
    Nessuno.
    Questa stored procedure non accetta parametri e non restituisce valori.

Esempio di Utilizzo:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Caricamento Layer Bronze';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Caricamento Tabelle CRM';
        PRINT '------------------------------------------------';

        -- Caricamento bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserimento Dati In: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Utente\Desktop\Corsi\SQL Server\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserimento Dati In: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Utente\Desktop\Corsi\SQL Server\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserimento Dati In: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Utente\Desktop\Corsi\SQL Server\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Caricamento Tabelle ERP';
        PRINT '------------------------------------------------';
        
        -- Caricamento bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserimento Dati In: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Utente\Desktop\Corsi\SQL Server\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserimento Dati In: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Utente\Desktop\Corsi\SQL Server\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserimento Dati In: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Utente\Desktop\Corsi\SQL Server\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Caricamento Layer Bronze Completato';
        PRINT '   - Durata Totale Caricamento: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' secondi';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERRORE DURANTE IL CARICAMENTO LAYER BRONZE';
        PRINT 'Messaggio Errore: ' + ERROR_MESSAGE();
        PRINT 'Numero Errore: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Stato Errore: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;

