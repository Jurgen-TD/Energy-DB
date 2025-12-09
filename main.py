# main.py
import datetime


def run_etl():
    now = datetime.datetime.now()
    print(f"ETL Job gestartet um: {now}")
    print("Dies ist ein Test für GitHub Actions.")
    # Hier kommt später der API-Code hin

if __name__ == "__main__":
    run_etl()
