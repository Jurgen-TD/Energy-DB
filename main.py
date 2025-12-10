# main.py
import requests
import pandas as pd
import datetime

# SMARD API Konfiguration
# ID 410: Tatsächliche Bruttostromerzeugung (Deutschland, alle Quellen)
SMARD_API_BASE_URL = "https://www.smard.de/app/chart_data/"
SMARD_DATA_ID = 410
REGION = "DE"
RESOLUTION = "hour" # Datenpunkte im 60-Minuten-Raster

# Die Anzahl der Tage, die wir von der API abrufen
DAYS_TO_FETCH = 30

def fetch_smard_data(data_id: int, region: str) -> dict:
    """Ruft die Rohdaten von der SMARD API für einen bestimmten Zeitraum ab."""
    print(f"Starte API-Abruf für Daten-ID: {data_id}")
    
    # NEUE DATUMSLOGIK
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=DAYS_TO_FETCH)
    
    # Umwandlung in Unix-Millisekunden-Zeitstempel (von der API benötigt)
    start_ts_ms = int(start_date.timestamp() * 1000)
    end_ts_ms = int(end_date.timestamp() * 1000)
    
    print(f"Abrufzeitraum: {start_date.strftime('%Y-%m-%d')} bis {end_date.strftime('%Y-%m-%d')}")
    
    # Die URL mit Start- und End-Parametern
    url = (
        f"{SMARD_API_BASE_URL}{data_id}/{region}/{data_id}_{region}_{RESOLUTION}_1627855200000.json"
#        f"?start={start_ts_ms}&end={end_ts_ms}" # WICHTIGE ÄNDERUNG
    )
    print("URL: ")
    print(url)

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # ZUSÄTZLICHER CHECK: Prüfen, ob der Schlüssel 'series' existiert und gefüllt ist
        if 'series' not in data or not data['series']:
            print("API-Antwort ist gültig, enthält aber keine 'series' Daten.")
            return {}
            
        print("API-Abruf erfolgreich.")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim API-Abruf: {e}")
        return {}


def transform_data(raw_data: dict) -> pd.DataFrame:
    """
    Wandelt die verschachtelte JSON-Struktur in ein flaches, 
    Tableau-freundliches DataFrame um.
    """
    if not raw_data or 'series' not in raw_data:
        print("Keine Rohdaten zum Transformieren gefunden.")
        return pd.DataFrame()

    print("Starte Datentransformation...")
    
    # 1. Daten in ein DataFrame konvertieren
    # Die Daten sind als Liste von [Timestamp, Wert] gespeichert
    all_data = []
    
#    for source in raw_data['series']:
#        print(source)
#        source_name = source[0]#['name']
#        source_unit = source[1]#['unit']
        # 'data' enthält die eigentlichen Zeitreihendaten
        
        # Iteriere durch jeden Datenpunkt und speichere ihn ab
#        for timestamp_ms, value in source['data']:
#            all_data.append({
#                'Timestamp_ms': timestamp_ms,
#                'Energiequelle': source_name,
#                'Leistung_MW': value if value is not None else 0
#            })


    for ts, value in raw_data['series']:
        if value is not None: 
            all_data.append({'Timestamp_ms': ts, 'Leistung_MW': value})

    df = pd.DataFrame(all_data)

    # 2. Zeitstempel formatieren
    df['DatumUhrzeit'] = pd.to_datetime(df['Timestamp_ms'], unit='ms')
    # Tableau bevorzugt oft getrennte Spalten für Datum und Zeit
    df['Datum'] = df['DatumUhrzeit'].dt.date
    df['Uhrzeit'] = df['DatumUhrzeit'].dt.time
    
    # 3. Berechnungen für den "Energiewende Tracker"
    # Die Summe der Leistung pro Zeitstempel ist die Gesamtlast
    total_power = df.groupby('DatumUhrzeit')['Leistung_MW'].sum().reset_index(name='Gesamtlast_MW')
    df = pd.merge(df, total_power, on='DatumUhrzeit')
    
    # Bestimme, welche Quellen "Erneuerbar" sind (SMARD-spezifisch)
    renewable_sources = ['Wind Offshore', 'Wind Onshore', 'Photovoltaik', 'Biomasse', 'Wasserkraft']
    
    # 4. Erneuerbare-Energien-Anteil berechnen
    # Neue Spalte: Ist die Quelle erneuerbar?
    #df['Is_Erneuerbar'] = df['Energiequelle'].apply(lambda x: 1 if x in renewable_sources else 0)
    
    # Summe der Erneuerbaren pro Zeitstempel
    #renewable_power = df[df['Is_Erneuerbar'] == 1].groupby('DatumUhrzeit')['Leistung_MW'].sum().reset_index(name='Erneuerbar_MW')
    
    # Gesamtdatenframe mergen und den Anteil berechnen
#    df = pd.merge(df, renewable_power, on='DatumUhrzeit', how='left').fillna(0)
#    df['Erneuerbar_Anteil'] = (df['Erneuerbar_MW'] / df['Gesamtlast_MW']) * 100
#    df['Erneuerbar_Anteil'] = df['Erneuerbar_Anteil'].replace([float('inf'), -float('inf')], 0).round(2)
    
    # Nur die wichtigsten Spalten für Tableau auswählen
 #   df_final = df[['DatumUhrzeit', 'Datum', 'Energiequelle', 'Leistung_MW', 'Gesamtlast_MW', 'Erneuerbar_MW', 'Erneuerbar_Anteil']].copy()
    
    # Bereinigen von Duplikaten (falls die API welche liefert) und sortieren
#    df_final.drop_duplicates(inplace=True)
#    df_final.sort_values(by=['DatumUhrzeit', 'Energiequelle'], inplace=True)

    print(f"Datentransformation abgeschlossen. {len(df)} Zeilen bereit.")
#    return df_final
    return df

def run_etl():
    """Der Hauptprozess, der auf GitHub Actions laufen wird."""
    start_time = datetime.datetime.now()
    print(f"--- ETL Job gestartet um: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    # 1. EXTRACT
    raw_data = fetch_smard_data(SMARD_DATA_ID, REGION)
    
    # 2. TRANSFORM
    clean_df = transform_data(raw_data)
    
    if clean_df.empty:
        print("Prozess beendet: Keine Daten zum Speichern.")
        return

    # 3. LOAD (Nächster Schritt: Google Sheets)
    # Zum Testen speichern wir die Daten als CSV, um sie auf GitHub zu prüfen.
    csv_file = 'smard_data.csv'
    # Wir überschreiben die Datei jedes Mal. Für Google Sheets nutzen wir später Append.
    clean_df.to_csv(csv_file, index=False)
    print(f"Daten erfolgreich in '{csv_file}' gespeichert.")
    
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"--- ETL Job erfolgreich abgeschlossen in {duration:.2f} Sekunden. ---")


if __name__ == "__main__":
    run_etl()
