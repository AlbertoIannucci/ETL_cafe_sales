from modello_base import ModelloBase
import pandas as pd
import numpy as np
import pymysql

class DatasetCleaner(ModelloBase):

    def __init__(self, dataset_path):
        self.dataframe = pd.read_csv(dataset_path)
        self.dataframe_sistemato = self.sistemazione()

    # Metodo di sistemazione
    def sistemazione(self):
        # Copia del dataframe
        df_sistemato = self.dataframe.copy()
        # Sostituzione nan camufatti
        df_sistemato = df_sistemato.replace("ERROR", np.nan).replace("UNKNOWN", np.nan)
        # Sostituzione nan per variabili categoriali
        variabili_categoriali = ["Item", "Payment Method", "Location"]
        for col in df_sistemato.columns:
            if col in variabili_categoriali:
                df_sistemato[col] = df_sistemato[col].fillna("Unknown")
        # Conversione variabili quantitative
        variabili_quantitative = ["Quantity", "Price Per Unit", "Total Spent"]
        for col in df_sistemato.columns:
            if col in variabili_quantitative:
                df_sistemato[col] = df_sistemato[col].astype(float)
        # Sostituzione nan con mediana per le variabili quantitative
        for col in df_sistemato.columns:
            if col in variabili_quantitative:
                df_sistemato[col] = df_sistemato[col].fillna(df_sistemato[col].median())
        # Conversione Quantity
        df_sistemato["Quantity"] = df_sistemato["Quantity"].astype(int)
        # Conversione Transaction Date
        df_sistemato["Transaction Date"] = pd.to_datetime(df_sistemato["Transaction Date"])
        # Drop valori nan in Transaction Date
        df_sistemato = df_sistemato.dropna(subset=["Transaction Date"])
        # Rimappatura etichette
        df_sistemato = df_sistemato.rename(columns={
            "Transaction ID":"transacrion_id",
            "Item":"item",
            "Quantity":"quantity",
            "Price Per Unit": "price_per_unit",
            "Total Spent":"total_spent",
            "Payment Method":"payment_method",
            "Location":"location",
            "Transaction Date":"transaction_date"
        })

        return df_sistemato

# Funzione per stabilire una connesione con il database
def getconnection():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="cafe"
    )

# Funzione per eseguire una query
def creazione_tabella():
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                sql =("CREATE TABLE IF NOT EXISTS cafe_sales("
                      "transacrion_id VARCHAR(30) PRIMARY KEY,"
                      "item VARCHAR(30) NOT NULL,"
                      "quantity INT NOT NULL,"
                      "price_per_unit FLOAT NOT NULL,"
                      "total_spent FLOAT NOT NULL,"
                      "payment_method VARCHAR(30) NOT NULL,"
                      "location VARCHAR(30) NOT NULL,"
                      "transaction_date DATE NOT NULL"
                      ");")
                cursor.execute(sql)
                connection.commit()
                return cursor.rowcount
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

# Funzione per esportare i dati nel database (senza intestazione)
def esporta_dati_db(df_sistemato):
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                for index, row in df_sistemato.iterrows():
                    # Preparazione della query di inserimento
                    sql = """
                        INSERT INTO cafe_sales (transacrion_id, item, quantity, price_per_unit, total_spent, payment_method, location, transaction_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    # Inserimento dei dati nella tabella
                    cursor.execute(sql, (
                        row['transacrion_id'],
                        row['item'],
                        row['quantity'],
                        row['price_per_unit'],
                        row['total_spent'],
                        row['payment_method'],
                        row['location'],
                        row['transaction_date']
                    ))
                connection.commit()  # Salvataggio delle modifiche nel database
                print("Dati esportati con successo.")
        finally:
         connection.close()
    except Exception as e:
        print(f"Errore nell'esportazione dei dati: {e}")

modello = DatasetCleaner("../Dataset/dataset.csv")
# Passo 1. Analisi generali del dataset
#modello.analisi_generali(modello.dataframe)
# Risultati:
# Osservazioni: 10000; Variabili: 8; Tipi: object; Valori nan: presenti
# Passo 2. Analisi dei valori univoci
#modello.analisi_valori_univoci(modello.dataframe, ["Transaction ID"])
# Risultati:
# Valori nan camuffati con ERROR e UNKNOWN
# Passo 3. Sostituzione valori nan camuffati
# Passo 4. Analisi generali del nuovo dataset
#modello.analisi_generali(modello.dataframe_sistemato)
# Risultati:
# Nessuna colonna da droppare. Proseguo alla sostituzione
# Passo 5. Sostiuzione valori nan per le variabili categoriali:
# Strategia: sostituisco i nan con una categoria 'Unknown'
#modello.analisi_valori_univoci(modello.dataframe_sistemato, ["Transaction ID"])
# Variabili categoriali: Item, Payment Method, Location
# Passo 6. Conversione variabili quantitative object in float64
# Passo 7. Analisi degli outliers pre sostituzione
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Transaction ID", "Item",
#                                                               "Payment Method", "Location","Transaction Date"])
# Risultato:
# Quantity: 0%
# Price Per Unit: 0%
# Total Spent: 2.59%
# Passo 8. Sostituzione valori nan per le variabili quantitative
# Strategia: sostiuisco i nan con la mediana
# Passo 9. Analisi degli outliers post sostituzione
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Transaction ID", "Item",
#                                                               "Payment Method", "Location","Transaction Date"])
# Risultato:
# Quantity: 0%
# Price Per Unit: 0%
# Total Spent: 2.59%
# Outliers non cresciuti e tutti sotto al 10/15%
# Passo 10. Conversione Quantity da float a int
# Passo 11. Conversione Transaction Date in formato data
# Passo 12. Drop valori nan in Transaction Date in quanto sono al di sotto del 5%
# Passo 13. Rimappatura etichette
# Passo 14. Stabilisco la connesione con il database
# Passo 15. Crea una tabella cafe_sales
#creazione_tabella()
# Passo 16. Load dei dati
#esporta_dati_db(modello.dataframe_sistemato)

