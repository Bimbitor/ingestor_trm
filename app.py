import streamlit as st
import pandas as pd
import psycopg
import os

# 1. Configuración de la página
st.set_page_config(page_title="Data Engineer TRM Monitor", page_icon="📈")

st.title("📈 Monitor de Divisas (USD Base)")
st.markdown("Pipeline automatizado con **Python 3.14**, **Supabase** y **GitHub Actions**.")

# 2. Conexión a la base de datos (Patrón Singleton para eficiencia)
def get_conn():
    # En local lee de os.environ, en la nube lee de st.secrets
    db_url = st.secrets["SUPABASE_DB_URL"] if "SUPABASE_DB_URL" in st.secrets else os.getenv("SUPABASE_DB_URL")
    return psycopg.connect(db_url, prepare_threshold=None)

# 3. Extracción de datos para visualización
@st.cache_data(ttl=3600) # Caché de 1 hora para no saturar Supabase (Costo-eficiencia)
def load_data():
    with get_conn() as conn:
        query = """
            SELECT currency_code, exchange_rate, reference_date, extracted_at 
            FROM trm_history 
            ORDER BY reference_date DESC 
            LIMIT 200;
        """
        df = pd.read_sql(query, conn)
    return df

try:
    df = load_data()

    # 4. Métricas Principales (KPIs)
    col1, col2, col3 = st.columns(3)
    
    # Filtrar solo COP para el KPI principal
    df_cop = df[df['currency_code'] == 'COP'].iloc[0]
    col1.metric("TRM Actual (COP)", f"${df_cop['exchange_rate']:.2f}", help="Última actualización de la API")
    col2.metric("Monedas Trackeadas", df['currency_code'].nunique())
    col3.metric("Última Ingesta", df['extracted_at'].iloc[0].strftime('%H:%M'))

    # 5. Gráfico de Tendencia
    st.subheader("Evolución de Tasa de Cambio")
    # Pivotamos el dataframe para que sea apto para gráficos
    chart_data = df.pivot(index='reference_date', columns='currency_code', values='exchange_rate')
    st.line_chart(chart_data)

    # 6. Tabla de datos crudos
    with st.expander("Ver Datos Crudos (Supabase)"):
        st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"Error cargando el Dashboard: {e}")