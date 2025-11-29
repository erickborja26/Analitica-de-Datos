import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import joblib

# Configuracion visual
st.set_page_config(page_title="Monitor Aire Lima", layout="wide")

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models')
RUTA_METRICAS = os.path.join(MODELS_DIR, 'metricas_integrante_1.csv')
RUTA_MODELO = os.path.join(MODELS_DIR, 'mejor_modelo_p1.pkl')
RUTA_MAPA = os.path.join(MODELS_DIR, 'mapa_estaciones.pkl')

# Funciones de carga
@st.cache_data
def cargar_datos():
    metricas = pd.read_csv(RUTA_METRICAS) if os.path.exists(RUTA_METRICAS) else None
    mapa = joblib.load(RUTA_MAPA) if os.path.exists(RUTA_MAPA) else None
    return metricas, mapa

@st.cache_resource
def cargar_modelo():
    return joblib.load(RUTA_MODELO) if os.path.exists(RUTA_MODELO) else None

# Cargar todo
df_metrics, mapa_estaciones = cargar_datos()
modelo = cargar_modelo()

st.title("☁️ Sistema de Predicción de Calidad de Aire")
st.markdown("Evaluación de Modelos de Ensemble Learning y Random Forests")

tab1, tab2 = st.tabs(["📊 Análisis Comparativo", "🤖 Simulador en Vivo"])

# --- PESTAÑA 1: RESULTADOS ---
with tab1:
    if df_metrics is not None:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.write("### Ranking de Modelos")
            df_sorted = df_metrics.sort_values("R2", ascending=False)
            st.dataframe(df_sorted.style.highlight_max(subset=["R2"]), use_container_width=True)
            
            ganador = df_sorted.iloc[0]
            st.success(f"🏆 Ganador: **{ganador['Modelo']}**")
            st.info(f"R2: {ganador['R2']:.4f} | RMSE: {ganador['RMSE']:.4f}")
            
        with col2:
            st.write("### Comparativa Visual")
            fig, ax = plt.subplots(figsize=(8, 5))
            colors = ['green' if x == ganador['Modelo'] else 'gray' for x in df_sorted['Modelo']]
            ax.barh(df_sorted['Modelo'], df_sorted['R2'], color=colors)
            ax.set_xlabel("R2 Score")
            st.pyplot(fig)
    else:
        st.error("No se encontraron métricas. Ejecuta el entrenamiento primero.")

# --- PESTAÑA 2: SIMULADOR ---
with tab2:
    if modelo and mapa_estaciones:
        st.header("Predicción por Zona")
        
        # Controles
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            hora = st.slider("Hora (0-23)", 0, 23, 18)
        with c2:
            mes = st.selectbox("Mes", range(1, 13), index=9)
        with c3:
            dia = st.selectbox("Día Semana", ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"])
            dia_num = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"].index(dia)
        with c4:
            # AQUÍ ESTÁ LA SOLUCIÓN: Mostramos nombres, usamos códigos
            nombres_disponibles = list(mapa_estaciones.keys())
            nombre_seleccionado = st.selectbox("Estación de Monitoreo", nombres_disponibles)
            codigo_estacion = mapa_estaciones[nombre_seleccionado]

        if st.button("Predecir Contaminación", type="primary"):
            # Input vector: [Hora, Mes, Dia, Estacion_Code]
            entrada = np.array([[hora, mes, dia_num, codigo_estacion]])
            
            try:
                pred = modelo.predict(entrada)[0] # Resultado: [PM2.5, PM10, SO2, NO2, O3, CO]
                targets = ['PM 2.5', 'PM 10', 'SO2', 'NO2', 'O3', 'CO']
                
                st.subheader(f"Pronóstico para: {nombre_seleccionado}")
                cols = st.columns(6)
                for i, (col, val, name) in enumerate(zip(cols, pred, targets)):
                    col.metric(name, f"{val:.1f}", "µg/m³")
                
                # Gráfico Radar/Barra
                st.bar_chart(pd.DataFrame({'Valor': pred}, index=targets))
                
            except Exception as e:
                st.error(f"Error en predicción: {e}")
    else:
        st.warning("Faltan archivos del modelo o el mapa de estaciones.")