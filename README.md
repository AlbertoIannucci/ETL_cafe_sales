# ETL_cafe_sales 📦🔄📊

Uno script Python completo per eseguire un processo ETL (Extract - Transform - Load) su dati di vendita, partendo da un file CSV grezzo fino al caricamento dei dati puliti in un database MySQL.

## ⚙️ Funzionalità

- **Caricamento e pulizia dei dati da file CSV**
- **Rilevamento e sostituzione di valori anomali** (`"ERROR"`, `"UNKNOWN"`)
- **Gestione dei valori mancanti**:
  - Per le **variabili categoriali**: sostituzione con `"Unknown"`
  - Per le **variabili quantitative**: imputazione tramite **mediana**
- **Conversione tipi di dati** (quantitativi e date)
- **Rimappatura delle etichette delle colonne** per coerenza con lo schema del database
- **Creazione automatica della tabella** MySQL se non esiste
- **Caricamento dei dati puliti** nel database
- **Estendibile tramite superclasse astratta `ModelloBase`** con metodi di analisi dei dati integrati

## 🧱 Architettura

Il modulo principale è composto da:
- `DatasetCleaner`: classe principale che eredita da `ModelloBase`, responsabile di tutta la logica di pulizia e trasformazione.
- Funzioni standalone per la **connessione al database**, **creazione della tabella** e **inserimento dei dati**.
