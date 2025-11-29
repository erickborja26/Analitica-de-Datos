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
RUTA_MODELOS_DICT = os.path.join(MODELS_DIR, 'todos_modelos_p1.pkl') # Ahora cargamos el dict completo
RUTA_MAPA = os.path.join(MODELS_DIR, 'mapa_estaciones.pkl')

# Funciones de carga
@st.cache_data
def cargar_datos_csv():
    metricas = pd.read_csv(RUTA_METRICAS) if os.path.exists(RUTA_METRICAS) else None
    mapa = joblib.load(RUTA_MAPA) if os.path.exists(RUTA_MAPA) else None
    return metricas, mapa

@st.cache_resource
def cargar_diccionario_modelos():
    if os.path.exists(RUTA_MODELOS_DICT):
        return joblib.load(RUTA_MODELOS_DICT) # Devuelve un diccionario {nombre: modelo}
    return None

# Cargar recursos
df_metrics, mapa_estaciones = cargar_datos_csv()
dict_modelos = cargar_diccionario_modelos()

st.title("☁️ Sistema de Predicción de Calidad de Aire")
st.markdown("Plataforma de Estimación de Contaminantes Atmosféricos en Lima usando Modelos Ensemble Learning y Random Forests")

tab1, tab2 = st.tabs(["📊 Comparativa de Rendimiento", "🤖 Simulador Interactivo"])

# --- PESTAÑA 1: RESULTADOS ---
with tab1:
    if df_metrics is not None:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("Ranking de Modelos")
            df_sorted = df_metrics.sort_values("R2", ascending=False)
            st.dataframe(df_sorted.style.highlight_max(subset=["R2"]), use_container_width=True)
            
            ganador = df_sorted.iloc[0]
            st.success(f"🏆 Mejor Modelo Global: **{ganador['Modelo']}**")
            
        with col2:
            st.subheader("Métrica R2 (Precisión)")
            fig, ax = plt.subplots(figsize=(8, 4))
            # Resaltar al ganador
            colors = ['#2ca02c' if x == ganador['Modelo'] else '#4c72b0' for x in df_sorted['Modelo']]
            ax.barh(df_sorted['Modelo'], df_sorted['R2'], color=colors)
            ax.set_xlabel("R2 Score (1.0 = Perfecto)")
            ax.set_xlim(0, 1.0)
            ax.grid(axis='x', linestyle='--', alpha=0.5)
            st.pyplot(fig)
    else:
        st.error("No se encontraron métricas. Ejecuta 'entrenamiento_modelos.py' primero.")

# --- PESTAÑA 2: SIMULADOR ---
with tab2:
    if dict_modelos and mapa_estaciones:
        
        # 1. SELECTOR DE MODELO
        st.markdown("### ⚙️ Configuración de la Simulación")
        # Obtenemos los nombres de los modelos disponibles
        lista_modelos = list(dict_modelos.keys())
        # Buscamos el mejor por defecto si tenemos las métricas, si no, el primero
        idx_default = 0
        if df_metrics is not None:
            mejor_nombre = df_metrics.sort_values("R2", ascending=False).iloc[0]['Modelo']
            if mejor_nombre in lista_modelos:
                idx_default = lista_modelos.index(mejor_nombre)
        
        modelo_seleccionado_nombre = st.selectbox("Seleccionar Algoritmo Predictivo:", lista_modelos, index=idx_default)
        modelo_activo = dict_modelos[modelo_seleccionado_nombre]
        
        st.markdown("---")
        
        # 2. CONTROLES DE ENTRADA (4 Variables: Hora, Mes, Dia, Estacion)
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            hora = st.slider("Hora del Día (0-23h)", 0, 23, 12)
        with c2:
            mes = st.selectbox("Mes del Año", range(1, 13), index=9, format_func=lambda x: f"Mes {x}")
        with c3:
            # Lista de dias para mostrar texto pero usar numero (0=Lunes)
            dias_txt = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            dia_txt = st.selectbox("Día de la Semana", dias_txt)
            dia_num = dias_txt.index(dia_txt)
        with c4:
            nombres_estaciones = list(mapa_estaciones.keys())
            estacion_txt = st.selectbox("Estación de Monitoreo", nombres_estaciones)
            estacion_code = mapa_estaciones[estacion_txt]

        # 3. BOTÓN DE PREDICCIÓN
        if st.button("Ejecutar Predicción", type="primary"):
            # Input vector: [Hora, Mes, Dia, Estacion]
            entrada = np.array([[hora, mes, dia_num, estacion_code]])
            
            try:
                # Predicción Multi-Output
                pred = modelo_activo.predict(entrada)[0]
                targets = ['PM 2.5', 'PM 10', 'SO2', 'NO2', 'O3', 'CO']
                
                st.subheader(f"Resultados con: {modelo_seleccionado_nombre}")
                
                # Tarjetas de metricas
                cols = st.columns(6)
                for i, (col, val, name) in enumerate(zip(cols, pred, targets)):
                    col.metric(name, f"{val:.2f}", "µg/m³")
                
                # Gráfico
                st.caption(f"Perfil de contaminación estimado para {estacion_txt} a las {hora}:00")
                chart_data = pd.DataFrame({'Concentración': pred}, index=targets)
                st.bar_chart(chart_data)
                
            except Exception as e:
                st.error(f"Error técnico: {e}")
                st.info("Intenta volver a entrenar los modelos si cambiaste la estructura de datos.")
            
    else:
        st.warning("⚠️ Faltan archivos del sistema. Ejecuta 'entrenamiento_modelos.py' para generar los modelos.")