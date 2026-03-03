import os
import requests
import psycopg
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_db_connection():
    # Usamos prepare_threshold=None para evitar el error de "prepared statement already exists"
    return psycopg.connect(os.getenv("SUPABASE_DB_URL"), prepare_threshold=None)

def fetch_bulk_history():
    print("📡 Consultando serie temporal real a Frankfurter (Jan 2026 - Present)...")
    # Endpoint de rango: /FECHA_INICIO..FECHA_FIN
    url = "https://api.frankfurter.app/2026-01-01..2026-02-28?from=USD&to=COP,EUR,MXN,BRL,PEN,CLP"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error de red: {e}")
        return None

def load_to_supabase(data):
    if not data or 'rates' not in data:
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            base = data['base']
            rates_dict = data['rates'] # Estructura: {"2026-01-01": {"EUR": 0.92, ...}, ...}
            
            print(f"📦 Procesando {len(rates_dict)} días de historia...")
            
            for date_str, currencies in rates_dict.items():
                for curr, val in currencies.items():
                    cur.execute("""
                        INSERT INTO trm_history (currency_code, base_currency, exchange_rate, reference_date, extracted_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (currency_code, reference_date) 
                        DO UPDATE SET exchange_rate = EXCLUDED.exchange_rate, extracted_at = EXCLUDED.extracted_at;
                    """, (curr, base, val, date_str, datetime.now()))
            
            conn.commit()
            print("✅ ¡Backfilling exitoso! Datos reales cargados.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en la carga: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    json_data = fetch_bulk_history()
    if json_data:
        load_to_supabase(json_data)