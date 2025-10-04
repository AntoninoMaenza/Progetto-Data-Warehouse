/*
===============================================================================
Controlli di Qualità - Layer Gold
===============================================================================
Scopo dello Script:
    Questo script esegue controlli di qualità per validare l'integrità, consistenza 
    e accuratezza del Layer Gold. Questi controlli verificano:
    - Unicità delle chiavi surrogate nelle tabelle dimensionali
    - Integrità referenziale tra tabelle di fatto e dimensioni
    - Validazione delle relazioni nel modello dati per scopi analitici

Note di Utilizzo:
    - Investigare e risolvere eventuali discrepanze trovate durante i controlli
===============================================================================
*/

-- ====================================================================
-- Controllo 'gold.dim_customers'
-- ====================================================================
-- Verifica Unicità della Chiave Cliente in gold.dim_customers
-- Risultato Atteso: Nessun Risultato
SELECT 
    customer_key,
    COUNT(*) AS conteggio_duplicati
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Controllo 'gold.dim_products'
-- ====================================================================
-- Verifica Unicità della Chiave Prodotto in gold.dim_products
-- Risultato Atteso: Nessun Risultato
SELECT 
    product_key,
    COUNT(*) AS conteggio_duplicati
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Controllo 'gold.fact_sales'
-- ====================================================================
-- Verifica la Connettività del Modello Dati tra Fatto e Dimensioni
-- Risultato Atteso: Nessun Risultato (tutte le chiavi devono esistere nelle dimensioni)
SELECT 
    f.order_number,
    f.customer_key,
    f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;