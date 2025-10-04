# Data Catalog per il Livello Gold

## Panoramica (Overview)
Il **Livello Gold** (Gold Layer) rappresenta i dati a livello di business, strutturati per supportare casi d'uso analitici e di reportistica. Consiste in **tabelle di dimensione** (*dimension tables*) e **tabelle di fatto** (*fact tables*) per specifiche metriche di business.

---

### 1. **gold.dim_customers**
- **Scopo (Purpose):** Memorizza i dettagli dei clienti arricchiti con dati demografici e geografici.
- **Colonne (Columns):**

| Column Name| Data Type | Descrizione |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer\_key| INT | **Chiave surrogata** (*Surrogate key*) che identifica in modo univoco ogni record cliente nella tabella di dimensione. |
| customer\_id| INT | Identificatore numerico univoco assegnato a ciascun cliente.|
| customer\_number| NVARCHAR(50)| Identificatore alfanumerico che rappresenta il cliente, usato per tracciamento e referenziazione. |
| first\_name | NVARCHAR(50)| Il nome del cliente, come registrato nel sistema.|
| last\_name| NVARCHAR(50)| Il cognome del cliente. |
| country| NVARCHAR(50)| Il paese di residenza del cliente (es. 'Australia'). |
| marital\_status | NVARCHAR(50)| Lo stato civile del cliente (es. 'Married', 'Single').|
| gender | NVARCHAR(50)| Il genere del cliente (es. 'Male', 'Female', 'n/a'). |
| birthdate| DATE| La data di nascita del cliente, formattata come AAAA-MM-GG (es. 1971-10-06). |
| create\_date| DATE| La data e l'ora in cui il record cliente è stato creato nel sistema. |

---

### 2. **gold.dim_products**
- **Scopo (Purpose):** Fornisce informazioni sui prodotti e i loro attributi.
- **Colonne (Columns):**

| Column Name | Data Type | Descrizione |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|
| product\_key| INT | **Chiave surrogata** (*Surrogate key*) che identifica in modo univoco ogni record prodotto nella tabella di dimensione prodotto. |
| product\_id| INT | Un identificatore univoco assegnato al prodotto per tracciamento e referenziazione interni.|
| product\_number| NVARCHAR(50)| Un codice alfanumerico strutturato che rappresenta il prodotto, spesso usato per la categorizzazione o l'inventario. |
| product\_name| NVARCHAR(50)| Nome descrittivo del prodotto, inclusi dettagli chiave come tipo, colore e taglia.|
| category\_id | NVARCHAR(50)| Un identificatore univoco per la categoria del prodotto, che si collega alla sua classificazione di alto livello. |
| category| NVARCHAR(50)| La classificazione più ampia del prodotto (es. Bikes, Components) per raggruppare articoli correlati. |
| subcategory | NVARCHAR(50)| Una classificazione più dettagliata del prodotto all'interno della categoria, come il tipo di prodotto. |
| maintenance\_required| NVARCHAR(50)| Indica se il prodotto richiede manutenzione (es. 'Yes', 'No'). |
| cost| INT | Il costo o prezzo base del prodotto, misurato in unità monetarie. |
| product\_line| NVARCHAR(50)| La linea o serie specifica di prodotti a cui appartiene il prodotto (es. Road, Mountain).|
| start\_date| DATE| La data in cui il prodotto è diventato disponibile per la vendita o l'uso. |

---

### 3. **gold.fact_sales**
- **Scopo (Purpose):** Memorizza i dati transazionali di vendita per scopi analitici.
- **Colonne (Columns):**

| Column Name | Data Type | Descrizione |
|-----------------|---------------|-----------------------------------------------------------------------------------------------|
| order\_number| NVARCHAR(50)| Un identificatore alfanumerico univoco per ogni ordine di vendita (es. 'SO54496'). |
| product\_key | INT | **Chiave surrogata** (*Surrogate key*) che collega l'ordine alla tabella di dimensione prodotto. |
| customer\_key| INT | **Chiave surrogata** (*Surrogate key*) che collega l'ordine alla tabella di dimensione cliente. |
| order\_date| DATE| La data in cui l'ordine è stato effettuato. |
| shipping\_date | DATE| La data in cui l'ordine è stato spedito al cliente.|
| due\_date| DATE| La data di scadenza del pagamento dell'ordine.|
| sales\_amount| INT | Il valore monetario totale della vendita per la riga d'ordine, in unità di valuta intera (es. 25). |
| quantity| INT | Il numero di unità del prodotto ordinato per la riga d'ordine (es. 1). |
| price | INT | Il prezzo per unità del prodotto per la riga d'ordine, in unità di valuta intera (es. 25).|