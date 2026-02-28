import os
import requests
import psycopg
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_db_connection():
    db_url = os.getenv("SUPABASE_DB_URL")
    return psycopg.connect(db_url, prepare_threshold=None)

def extract_trm():
    print("Extrayendo datos de Open ER-API (Soporta COP)...")
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def transform(data):
    print("Transformando datos con precisión técnica...")
    rates = data['rates']
    
    unix_time = data.get('time_last_update_unix')
    
    if unix_time:
        date_obj = datetime.fromtimestamp(unix_time)
        date_str = date_obj.strftime('%Y-%m-%d')
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    base = data['base_code']
    target_currencies = ['COP', 'MXN', 'EUR', 'BRL']
    now = datetime.now()
    
    records = []
    for currency in target_currencies:
        if currency in rates:
            records.append((
                currency, 
                base, 
                rates[currency], 
                date_str,
                now
            ))
    return records

def load_upsert_native(records):
    if not records:
        print("No hay registros para cargar.")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            upsert_query = """
            INSERT INTO trm_history (currency_code, base_currency, exchange_rate, reference_date, extracted_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (currency_code, reference_date) 
            DO UPDATE SET 
                exchange_rate = EXCLUDED.exchange_rate,
                extracted_at = EXCLUDED.extracted_at;
            """
            cur.executemany(upsert_query, records)
        conn.commit()
        print(f"¡Éxito! {len(records)} monedas procesadas.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        raw_data = extract_trm()
        clean_data = transform(raw_data)
        load_upsert_native(clean_data)
    except Exception as e:
        print(f"ERROR: {e}")
        exit(1)