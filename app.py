import streamlit as st
import pandas as pd
import psycopg
import os
from datetime import datetime

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="Global Currency Monitor | Data Pipeline",
    page_icon="📈",
    layout="wide"
)

# Estilo CSS personalizado para mejorar la visualización
st.markdown("""
    <style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
    """, unsafe_allow_html=True) # <--- Cámbialo a esto

# --- CAPA DE DATOS (DATA ACCESS LAYER) ---
def get_conn_string():
    """Gestiona credenciales tanto en local (.env) como en Streamlit Cloud (Secrets)"""
    if "SUPABASE_DB_URL" in st.secrets:
        return st.secrets["SUPABASE_DB_URL"]
    return os.getenv("SUPABASE_DB_URL")

@st.cache_data(ttl=3600) # Caché de 1 hora para optimizar recursos
def fetch_data_from_supabase():
    conn_url = get_conn_string()
    if not conn_url:
        st.error("Missing Database URL. Please check your secrets/env.")
        return pd.DataFrame()
    
    try:
        # Usamos el modo prepare_threshold=None por compatibilidad con PgBouncer
        with psycopg.connect(conn_url, prepare_threshold=None) as conn:
            query = """
                SELECT 
                    currency_code, 
                    exchange_rate, 
                    reference_date, 
                    extracted_at 
                FROM trm_history 
                ORDER BY reference_date ASC;
            """
            df = pd.read_sql(query, conn)
            # Convertimos a tipos de datos correctos
            df['reference_date'] = pd.to_datetime(df['reference_date'])
            df['exchange_rate'] = pd.to_numeric(df['exchange_rate'])
            return df
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame()

# --- LÓGICA DE TRANSFORMACIÓN (BUSINESS LOGIC) ---
def process_analytics(df):
    """Calcula métricas de volatilidad y cambios porcentuales"""
    if df.empty:
        return df
    
    # Ordenar para cálculos temporales
    df = df.sort_values(['currency_code', 'reference_date'])
    
    # Calcular cambio porcentual respecto al registro anterior de cada moneda
    df['prev_rate'] = df.groupby('currency_code')['exchange_rate'].shift(1)
    df['pct_change'] = ((df['exchange_rate'] - df['prev_rate']) / df['prev_rate']) * 100
    
    return df

# --- INTERFAZ DE USUARIO (UI) ---
def main():
    st.title("📊 Global Currency Analytics Dashboard")
    st.caption(f"Pipeline Status: Active | Python 3.14 | Last Sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Carga y Procesamiento
    raw_df = fetch_data_from_supabase()
    df = process_analytics(raw_df)

    if df.empty:
        st.warning("No data found in the database. Check your ETL pipeline.")
        return

    # --- SECCIÓN 1: KPIs DE ALTO NIVEL ---
    st.subheader("Real-Time Market Pulse")
    target_currencies = ['COP', 'MXN', 'EUR', 'BRL']
    cols = st.columns(len(target_currencies))

    for i, curr in enumerate(target_currencies):
        curr_data = df[df['currency_code'] == curr]
        if not curr_data.empty:
            last_record = curr_data.iloc[-1]
            change = last_record['pct_change']
            
            # Formateo de moneda según el tipo
            val_format = f"{last_record['exchange_rate']:,.2f}"
            
            cols[i].metric(
                label=f"USD / {curr}",
                value=val_format,
                delta=f"{change:.2f}%" if pd.notnull(change) else "New",
                delta_color="inverse" # En divisas, el incremento suele ser negativo para el comprador local
            )

    st.divider()

    # --- SECCIÓN 2: ANÁLISIS VISUAL ---
    col_chart, col_info = st.columns([3, 1])

    with col_chart:
        st.subheader("Price Convergence & Trends")
        # Pivotar para el gráfico de líneas
        chart_data = df.pivot(index='reference_date', columns='currency_code', values='exchange_rate')
        
        # Nota técnica: Si hay pocos datos, las líneas serán rectas. 
        # Mostramos puntos para que se note que son registros discretos.
        st.line_chart(chart_data)
        st.caption("Note: Straight lines indicate linear interpolation between daily snapshots.")

    with col_info:
        st.subheader("Market Insights")
        st.info("""
            **Why straight lines?**
            As a Data Architect, I've designed this as a **Daily Batch Pipeline**. 
            To see smooth curves, we need more data points over time. 
            The pipeline is currently enriching the database every 24 hours.
        """)
        
        # Mostrar el mayor movimiento del día
        if not df['pct_change'].dropna().empty:
            max_move = df.loc[df['pct_change'].abs().idxmax()]
            st.warning(f"🔥 **Highest Volatility:** {max_move['currency_code']} with {max_move['pct_change']:.2f}%")

    # --- SECCIÓN 3: DATA INSPECTION ---
    with st.expander("🔍 Warehouse Inspection (Raw Records from Supabase)"):
        st.write("This table shows the data exactly as it is stored in our PostgreSQL instance.")
        st.dataframe(
            df.sort_values('reference_date', ascending=False),
            use_container_width=True
        )

if __name__ == "__main__":
    main()