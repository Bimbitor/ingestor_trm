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

# Estilo CSS personalizado
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric {
        background-color: #000011;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    /* Estilo para el contenedor del convertidor */
    .converter-box {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e6e9ef;
    }
    </style>
    """, unsafe_allow_html=True)

# --- CAPA DE DATOS (DATA ACCESS LAYER) ---
def get_conn_string():
    if "SUPABASE_DB_URL" in st.secrets:
        return st.secrets["SUPABASE_DB_URL"]
    return os.getenv("SUPABASE_DB_URL")

@st.cache_data(ttl=3600)
def fetch_data_from_supabase():
    conn_url = get_conn_string()
    if not conn_url:
        st.error("Missing Database URL. Please check your secrets/env.")
        return pd.DataFrame()
    
    try:
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
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)

            df['reference_date'] = pd.to_datetime(df['reference_date'])
            df['exchange_rate'] = pd.to_numeric(df['exchange_rate'])
            return df
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame()

# --- LÓGICA DE TRANSFORMACIÓN ---
def process_analytics(df):
    if df.empty: return df
    df = df.sort_values(['currency_code', 'reference_date'])
    df['prev_rate'] = df.groupby('currency_code')['exchange_rate'].shift(1)
    df['pct_change'] = ((df['exchange_rate'] - df['prev_rate']) / df['prev_rate']) * 100
    return df

# --- INTERFAZ DE USUARIO (UI) ---
def main():
    st.title("📊 Global Currency Analytics Dashboard")
    st.caption(f"Pipeline Status: Active | Python 3.14 | Last Sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    raw_df = fetch_data_from_supabase()
    df = process_analytics(raw_df)

    if df.empty:
        st.warning("No data found in the database.")
        return

    # --- SECCIÓN 1: KPIs ---
    st.subheader("Real-Time Market Pulse")
    target_currencies = ['COP', 'MXN', 'EUR', 'BRL', 'PEN', 'CLP']
    cols = st.columns(len(target_currencies))

    for i, curr in enumerate(target_currencies):
        curr_data = df[df['currency_code'] == curr]
        if not curr_data.empty:
            last_record = curr_data.iloc[-1]
            change = last_record['pct_change']
            val_format = f"{last_record['exchange_rate']:,.2f}"
            cols[i].metric(
                label=f"USD / {curr}",
                value=val_format,
                delta=f"{change:.2f}%" if pd.notnull(change) else "New",
                delta_color="inverse"
            )

    st.divider()

    # --- NUEVA SECCIÓN: CALCULADORA DE CONVERSIÓN (MODULO DE VALOR) ---
    st.subheader("🧮 Currency to USD Converter")
    
    # Obtenemos solo las tasas más recientes para el cálculo
    latest_rates = df.sort_values('reference_date').groupby('currency_code').tail(1)
    
    with st.container():
        # Creamos dos columnas para el input del usuario
        c1, c2, c3 = st.columns([2, 2, 3])
        
        with c1:
            selected_currency = st.selectbox(
                "Select Currency to Convert",
                options=latest_rates['currency_code'].unique(),
                index=0
            )
        
        with c2:
            amount = st.number_input(
                f"Amount in {selected_currency}",
                min_value=0.0,
                value=1000.0,
                step=100.0
            )
            
        with c3:
            # Lógica de cálculo
            rate_row = latest_rates[latest_rates['currency_code'] == selected_currency]
            if not rate_row.empty:
                current_rate = float(rate_row['exchange_rate'].values[0])
                usd_value = amount / current_rate
                
                # Mostrar el resultado de forma destacada
                st.write("") # Espaciador
                st.success(f"### Total: ${usd_value:,.2f} USD")
                st.caption(f"Based on latest rate: 1 USD = {current_rate:,.2f} {selected_currency}")

    st.divider()

    # --- SECCIÓN 3: ANÁLISIS VISUAL ---
    col_chart, col_info = st.columns([3, 1])

    with col_chart:
        st.subheader("Price Convergence & Trends")
        chart_data = df.pivot(index='reference_date', columns='currency_code', values='exchange_rate')
        st.line_chart(chart_data)

    with col_info:
        st.subheader("Market Insights")
        if not df['pct_change'].dropna().empty:
            max_move = df.loc[df['pct_change'].abs().idxmax()]
            st.warning(f"🔥 **Highest Volatility:** {max_move['currency_code']} with {max_move['pct_change']:.2f}%")

    # --- SECCIÓN 4: DATA INSPECTION ---
    with st.expander("🔍 Warehouse Inspection"):
        st.dataframe(df.sort_values('reference_date', ascending=False), use_container_width=True)

if __name__ == "__main__":
    main()