/*
=============================================================
Creazione Database e Schemi
=============================================================
Scopo dello Script:
    Questo script crea un nuovo database denominato 'DataWarehouse' dopo aver verificato 
    se esiste già. Se il database esiste, viene eliminato e ricreato. Inoltre, lo script 
    configura tre schemi all'interno del database: 'bronze', 'silver' e 'gold'.
	
ATTENZIONE:
    L'esecuzione di questo script eliminerà completamente il database 'DataWarehouse' 
    se esistente. Tutti i dati nel database verranno eliminati permanentemente. 
    Procedere con cautela e assicurarsi di disporre di backup appropriati prima 
    di eseguire questo script.
*/

USE master;
GO

-- Elimina e ricrea il database 'DataWarehouse'
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Crea il database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creazione degli Schemi
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
