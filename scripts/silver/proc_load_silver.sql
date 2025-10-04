/*
===============================================================================
Stored Procedure: Caricamento Layer Silver (Bronze -> Silver)
===============================================================================
Scopo dello Script:
    Questa stored procedure esegue il processo ETL (Extract, Transform, Load) per 
    popolare le tabelle dello schema 'silver' dallo schema 'bronze'.
    Azioni Eseguite:
        - Svuota le tabelle Silver.
        - Inserisce dati trasformati e puliti da Bronze nelle tabelle Silver.
        
Parametri:
    Nessuno.
    Questa stored procedure non accetta parametri e non restituisce valori.

Esempio di Utilizzo:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Caricamento Layer Silver';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Caricamento Tabelle CRM';
        PRINT '------------------------------------------------';

        -- Caricamento silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserimento Dati In: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_marital_status, 
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status, -- Normalizza i valori dello stato civile in formato leggibile
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr, -- Normalizza i valori del genere in formato leggibile
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1; -- Seleziona il record più recente per cliente
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserimento Dati In: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Estrae l'ID categoria
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Estrae la chiave prodotto
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line, -- Mappa i codici linea prodotto a valori descrittivi
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
                AS DATE
            ) AS prd_end_dt -- Calcola la data di fine come un giorno prima della successiva data di inizio
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserimento Dati In: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
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
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales, -- Ricalcola le vendite se il valore originale è mancante o errato
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price  -- Deriva il prezzo se il valore originale non è valido
            END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Caricamento Tabelle ERP';
        PRINT '------------------------------------------------';

        -- Caricamento silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserimento Dati In: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Rimuove il prefisso 'NAS' se presente
                ELSE cid
            END AS cid, 
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate, -- Imposta a NULL le date di nascita future
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen -- Normalizza i valori del genere e gestisce i casi sconosciuti
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        -- Caricamento silver.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserimento Dati In: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid, 
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry -- Normalizza e gestisce i codici paese mancanti o vuoti
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';
        
        -- Caricamento silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Svuotamento Tabella: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserimento Dati In: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
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
        FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Durata Caricamento: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' secondi';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Caricamento Layer Silver Completato';
        PRINT '   - Durata Totale Caricamento: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' secondi';
        PRINT '==========================================';
        
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERRORE DURANTE IL CARICAMENTO LAYER SILVER';
        PRINT 'Messaggio Errore: ' + ERROR_MESSAGE();
        PRINT 'Numero Errore: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Stato Errore: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO