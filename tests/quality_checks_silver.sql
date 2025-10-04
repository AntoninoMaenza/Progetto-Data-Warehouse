/*
===============================================================================
Controlli di Qualità
===============================================================================
Scopo dello Script:
    Questo script esegue vari controlli di qualità per verificare consistenza, 
    accuratezza e standardizzazione dei dati nel layer 'silver'. Include controlli per:
    - Chiavi primarie NULL o duplicate
    - Spazi indesiderati nei campi stringa
    - Standardizzazione e consistenza dei dati
    - Range e ordini di date non validi
    - Consistenza dei dati tra campi correlati

Note di Utilizzo:
    - Eseguire questi controlli dopo il caricamento del Layer Silver
    - Investigare e risolvere eventuali discrepanze trovate durante i controlli
===============================================================================
*/

-- ====================================================================
-- Controllo 'silver.crm_cust_info'
-- ====================================================================
-- Verifica NULL o Duplicati nella Chiave Primaria
-- Risultato Atteso: Nessun Risultato
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Verifica Spazi Indesiderati
-- Risultato Atteso: Nessun Risultato
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Standardizzazione e Consistenza Dati
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Controllo 'silver.crm_prd_info'
-- ====================================================================
-- Verifica NULL o Duplicati nella Chiave Primaria
-- Risultato Atteso: Nessun Risultato
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Verifica Spazi Indesiderati
-- Risultato Atteso: Nessun Risultato
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Verifica NULL o Valori Negativi nei Costi
-- Risultato Atteso: Nessun Risultato
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Standardizzazione e Consistenza Dati
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Verifica Ordine Date Non Valido (Data Inizio > Data Fine)
-- Risultato Atteso: Nessun Risultato
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Controllo 'silver.crm_sales_details'
-- ====================================================================
-- Verifica Date Non Valide
-- Risultato Atteso: Nessuna Data Non Valida
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Verifica Ordine Date Non Valido (Data Ordine > Data Spedizione/Scadenza)
-- Risultato Atteso: Nessun Risultato
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Verifica Consistenza Dati: Vendite = Quantità * Prezzo
-- Risultato Atteso: Nessun Risultato
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Controllo 'silver.erp_cust_az12'
-- ====================================================================
-- Identifica Date Fuori Range
-- Risultato Atteso: Date di Nascita tra 1924-01-01 e Oggi
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Standardizzazione e Consistenza Dati
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Controllo 'silver.erp_loc_a101'
-- ====================================================================
-- Standardizzazione e Consistenza Dati
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Controllo 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Verifica Spazi Indesiderati
-- Risultato Atteso: Nessun Risultato
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Standardizzazione e Consistenza Dati
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;