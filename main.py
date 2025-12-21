import requests
import pandas as pd
import datetime
import matplotlib
import matplotlib.pyplot as plt
import os
import gspread # import google spread connector
import json
import base64

# Name der Google Sheet Datei
GOOGLE_SHEET_NAME = "SMARD Energy Data" # muss ich noch anpassen

# SMARD API Konfiguration
# ID 410: Tatsächliche Bruttostromerzeugung (Deutschland, alle Quellen)
SMARD_API_BASE_URL = "https://www.smard.de/app/chart_data/"
REGION = "DE" # in the first place we focus on Germany
RESOLUTION = "hour" # Datenpunkte im 60-Minuten-Raster
BLOCKS_TO_FETCH = 2 # the number of blocks to be fetched / one block is one week 

# SMARD DATA IDs:
# As I understood it is not possible to fetch several parameters at the same time
# instead multiple requests must be carried out
# We always have to check availability of data blocks for each filter / parameter
SMARD_FILTER = {
    'NETZLAST': 410,  # overall consumption
    'BRAUNKOHLE': 1223, 
    'WINDOFFSHORE': 1225,
    'WATER': 1226,    # Wasserkraft
    'FOSSIL_MISC': 1227,
    'RENEWABLE_MISC': 1228,
    'BIOGAS': 4066,
    'WINDONSHORE': 4067,
    'SOLAR': 4068,
    'STEINKOHLE': 4069,
    'PUMPSTORAGE': 4070,
    'GAS': 4071,
    'RESIDUALLOAD': 4359,
    'PUMPEDCONSUMED': 4387,
    'PRICE_DE': 4169,
    'PRICE_BG': 4996,
    'PRICE_NW': 4997,
    'PRICE_AU': 4170, # Austria
    'PRICE_FR': 254,
    'PRICE_PL': 258,
    'PRICE_IT': 255,
    'PRICE_CH': 259,  # Switzerland
    'PRICE_UN': 262   # Hungary
}


###########################################################
#   Frage die SMARD API nach den verfügbaren ladbaren Blöcken an
###########################################################
def get_available_blocks(data_id: int, region: str) -> list:
    """
    Lädt die verfügbaren Zeitpunkte (Blöcke, die geladen werden können)
    """
    url = f"{SMARD_API_BASE_URL}/{data_id}/{region}/index_{RESOLUTION}.json"
    
    try:
        r = requests.get(url, timeout=15)
        print(f" -> Status {r.status_code}, Content-Type: {r.headers.get('Content-Type')}")
        data = r.json()
        timestamps = data["timestamps"]
        print(f"Gefundene Timestamps: {len(timestamps)} Stück")
        return timestamps
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim API-Abruf: {e}")
        return []


###########################################################
#   Holt einen Daten-Block von der SMARD API und gibt diesen als einfache Liste zurück  
###########################################################
def fetch_smard_data(data_id: int, region: str, block: int) -> list:
    """Ruft die Rohdaten von der SMARD API für einen bestimmten Block ab."""
    # Die URL mit Start- und End-Parametern
    url = (f"{SMARD_API_BASE_URL}{data_id}/{region}/{data_id}_{region}_{RESOLUTION}_{block}.json")
    print(f"FETCH! Data: {data_id} URL: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # ZUSÄTZLICHER CHECK: Prüfen, ob der Schlüssel 'series' existiert und gefüllt ist
        if 'series' not in data or not data['series']:
            print("API-Antwort ist gültig, enthält aber keine 'series' Daten.")
            return []
        return data["series"]
        
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim API-Abruf: {e}")
        return []


###########################################################
#   Wandelt die Liste von der SMARD -API in ein für die Verarbeitung optimiertes DataFrame 
###########################################################
def transform_data(raw_data: list) -> pd.DataFrame:
    if not raw_data:
        print("Keine Rohdaten zum Transformieren gefunden.")
        return pd.DataFrame()

    print("Starte Datentransformation...")
    
    # Step 0: Daten in ein DataFrame konvertieren
    # Als Zwischenschritt werden die rohen Daten in ein Dict überführt und mit Überschriften versehen
    # Die Daten sind noch im Long-Format 
    # Die Daten kommen als Liste im Format: [Timestamp, Filter, Wert] 
    all_data = []
    for ts, fltr, value in raw_data:
        all_data.append({'Timestamp': ts, 'Filter': fltr, 'Value': value})
    df = pd.DataFrame(all_data)
    #df.drop_duplicates(subset=['Timestamp', 'Filter', 'Value'], inplace=True)

    # Step 1: bring data from Long- into Wide- format
    df = df.pivot(index=['Timestamp'], columns='Filter', values='Value')
    df = df.rename_axis(columns=None).reset_index()

    # Step 2: Bringe die Daten in eine einheitliche Reihenfolge und ersetze Timestamp in lesbares Format
    df.sort_values(by=['Timestamp'], inplace=True)
    df.insert(0,'DatumUhrzeit',pd.to_datetime(df['Timestamp'], unit='ms'))
    df.drop(columns=['Timestamp'], inplace=True)

    # Step 3: Bereinigen von Duplikaten (falls die API welche liefert) und sortieren
    df.drop_duplicates(inplace=True)

    # Step 4: Berechne die Summen der erneuerbaren und der fossilen Energien und deren Anteile
    df['Total_Renew'] = (df['WINDOFFSHORE'] + df['WINDONSHORE'] + df['WATER'] + df['BIOGAS'] + df['SOLAR'] + df['PUMPSTORAGE'] + df['RENEWABLE_MISC']).round(2)
    df['Total_Fossil'] = (df['BRAUNKOHLE'] + df['STEINKOHLE'] + df['GAS'] + df['FOSSIL_MISC']).round(2)
    df['Renew_Perc'] = ((df['Total_Renew'] / df['NETZLAST']) * 100).round(2)
    df['Fossil_Perc'] = ((df['Total_Fossil'] / df['NETZLAST']) * 100).round(2)
    #print(df.head()) # zum Debuggen können wir die Daten jetzt schon screenen
    #df.plot(x='DatumUhrzeit')
    #plt.show()

    print(f"Datentransformation abgeschlossen. {len(df)} Zeilen bereit.")
    return df

#############################################################
# Daten zu Google Sheets laden
# Stellt eine Verbindung zu Google Sheets über das GitHub Secret her und hängt die Daten an.
#############################################################
def load_to_google_sheets(df: pd.DataFrame):
    print("Open connection to Google Sheets...")
    
    # 1. Secret aus Umgebungsvariable lesen
    gcp_credentials_json_str = os.environ.get('GCP_CREDENTIALS')
    if not gcp_credentials_json_str:
        print("FEHLER: GCP_CREDENTIALS Secret nicht gefunden. Laden abgebrochen.")
        return

    # 2. JSON Key in ein Python-Objekt wandeln
    try:
        credentials = json.loads(gcp_credentials_json_str)
    except json.JSONDecodeError as e:
        print(f"FEHLER: Secret-JSON konnte nicht geparsed werden: {e}")
        return

    # 3. Authentifizierung und Verbindung
    try:
        # Hier authentifizieren ich mich mit dem JSON Key
        gc = gspread.service_account_from_dict(credentials)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        print(f"Verbindung zu Google Sheet '{GOOGLE_SHEET_NAME}' erfolgreich.")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"FEHLER: Google Sheet '{GOOGLE_SHEET_NAME}' nicht gefunden. Prüfen Sie den Namen und die Freigabe des Service Accounts.")
        return
    except Exception as e:
        print(f"FEHLER bei der Authentifizierung oder Verbindung: {e}")
        return

    # 4. Daten vorbereiten und anhängen
    # Konvertieren des DataFrame in eine zwei Listen (Header + Daten)
    header = df.columns.tolist()
    data_to_append = df.values.tolist()
    # Da ich immer alles neu lade, leere ich die Tabelle zuerst (Trunkate & Load)
    worksheet.clear()
    print("Sheet geleert (Truncate).")
    
    # 5. Header und Daten gemeinsam laden
    worksheet.update([header], 'A1')
    print("Header in Zeile 1 geschrieben.")
    worksheet.append_rows(data_to_append)
    
    print(f"Erfolgreich {len(data_to_append)} Zeilen in Google Sheets geschrieben.")


###########################################################
#   Main routine for "Extract - Transform - Load 
###########################################################
def run_etl():
    """Der Hauptprozess, der auf GitHub Actions laufen wird."""
    start_time = datetime.datetime.now()
    print(f"--- ETL Job gestartet um: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    filter_lst = [SMARD_FILTER['NETZLAST'],
                  SMARD_FILTER['BRAUNKOHLE'],
                  SMARD_FILTER['STEINKOHLE'],
                  SMARD_FILTER['GAS'],
                  SMARD_FILTER['FOSSIL_MISC'],
                  SMARD_FILTER['WINDOFFSHORE'],
                  SMARD_FILTER['WINDONSHORE'],
                  SMARD_FILTER['WATER'],    
                  SMARD_FILTER['BIOGAS'],
                  SMARD_FILTER['SOLAR'],
                  SMARD_FILTER['PUMPSTORAGE'],
                  SMARD_FILTER['RENEWABLE_MISC'], 
                  #SMARD_FILTER['PRICE_DE'],
                  #SMARD_FILTER['PRICE_PL']
                 ]
    raw_data = []
    raw_data_frame = []

#-- 1. EXTRACT
    for fltr in filter_lst:
        # 1.1 request list with available blocks
        blocks = get_available_blocks(fltr, REGION)
        blocks.sort(reverse=True)
        # 1.2 fetch data
        for block in blocks[0:BLOCKS_TO_FETCH]:
            raw_data.extend(fetch_smard_data(fltr, REGION, block)) # be aware using the "extend" method here!
        
        f = [k for k, v in SMARD_FILTER.items() if v == fltr][0] ## !!ugly but it works (getting keys for filter values from dict))
        for ts, value in raw_data:
            raw_data_frame.append([ts, f, value])

        raw_data.clear() # clear raw data block to be filled for next iteration

#-- 2. TRANSFORM
    df_clean = transform_data(raw_data_frame)
    
    if df_clean.empty:
        print("Prozess beendet: Keine Daten zum Speichern.")
        return

#-- 3. LOAD
    # 3.1 - Zum Testen speichere ich die Daten als CSV, um sie lokal oder auf GitHub zu prüfen.
    csv_file = 'smard_data.csv'
    df_clean.to_csv(csv_file, index=False)
    print(f"Daten erfolgreich lokal in '{csv_file}' gespeichert.")

    # 3.2 Für das Laden in Google Sheets muss ich das Datum in ein ISO-Format ändern, damit der JSON-Parser damit umgehen kann
    # Ich "klone" das DataFrame, bevor es geändert wird
    df_load = df_clean.copy() 
    df_load = df_load.fillna(0)
    # Konvertiert alle Timestamps in ISO-String-Format, das JSON-kompatibel ist
    df_load['DatumUhrzeit'] = df_load['DatumUhrzeit'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 3.3 Tableau arbeitet lieber im Longformat, sodass ich vor dem Laden zu Google Sheets eine Transformation vornehme
    df_long = df_load.melt(
        id_vars=['DatumUhrzeit'], 
        value_vars=['NETZLAST', 'BRAUNKOHLE', 'STEINKOHLE', 'GAS', 'FOSSIL_MISC', 'WINDOFFSHORE', 'WINDONSHORE', 'WATER', 'BIOGAS', 'SOLAR', 'PUMPSTORAGE', 'RENEWABLE_MISC', 'Renew_Perc', 'Fossil_Perc'], 
        var_name='Energiequelle', 
        value_name='Werte'
        )
    load_to_google_sheets(df_long) # Auskommentieren, wenn ich lokal teste !

    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"--- ETL Job erfolgreich abgeschlossen in {duration:.2f} Sekunden. ---")


if __name__ == "__main__":
    run_etl()
