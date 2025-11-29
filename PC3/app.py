import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
import joblib

# Configuracion visual
st.set_page_config(page_title="Monitor Aire Lima", layout="wide")

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models')
RUTA_METRICAS = os.path.join(MODELS_DIR, 'metricas_integrante_1.csv')
RUTA_MODELOS_DICT = os.path.join(MODELS_DIR, 'todos_modelos_p1.pkl')  # Diccionario de modelos multioutput
RUTA_MAPA = os.path.join(MODELS_DIR, 'mapa_estaciones.pkl')
RUTA_METRICAS_GLOBAL = os.path.join(MODELS_DIR, 'metricas_globales.csv')
RUTA_CALIDAD_AIRE = os.path.join(BASE_DIR, 'calidad_aire.csv')

# Funciones de carga
@st.cache_data
def cargar_datos_csv():
    metricas = pd.read_csv(RUTA_METRICAS) if os.path.exists(RUTA_METRICAS) else None
    mapa = joblib.load(RUTA_MAPA) if os.path.exists(RUTA_MAPA) else None
    return metricas, mapa

@st.cache_resource
def cargar_diccionario_modelos():
    if os.path.exists(RUTA_MODELOS_DICT):
        return joblib.load(RUTA_MODELOS_DICT)  # Devuelve un diccionario {nombre: modelo}
    return None

@st.cache_data
def cargar_metricas_globales():
    if os.path.exists(RUTA_METRICAS_GLOBAL):
        df = pd.read_csv(RUTA_METRICAS_GLOBAL)
        def limpiar_target(val):
            base = val.replace("_next_hour", "")
            base = base.replace("PM2_5", "PM 2.5").replace("PM10", "PM 10")
            return base
        df["Contaminante"] = df["target"].apply(limpiar_target)
        df["Modelo"] = df["modelo"].str.upper()
        return df
    return None

@st.cache_resource
def cargar_modelos_globales():
    modelos = {}
    if not os.path.exists(MODELS_DIR):
        return modelos
    for fname in os.listdir(MODELS_DIR):
        if not fname.startswith("modelo_global_") or "extra" in fname or not fname.endswith(".pkl"):
            continue
        nombre = fname.replace("modelo_global_", "").replace(".pkl", "")
        if not nombre.endswith("_next_hour"):
            continue
        partes = nombre.split("_")
        if len(partes) < 3:
            continue
        algoritmo = partes[0]
        target = "_".join(partes[1:])
        try:
            modelos.setdefault(algoritmo, {})[target] = joblib.load(os.path.join(MODELS_DIR, fname))
        except Exception:
            continue
    return modelos

@st.cache_data
def cargar_plantilla_global():
    if not os.path.exists(RUTA_CALIDAD_AIRE):
        return None
    try:
        from modelo_global import cargar_y_preparar
    except Exception:
        return None
    df_model = cargar_y_preparar(Path(RUTA_CALIDAD_AIRE))
    target_cols = [c for c in df_model.columns if c.endswith("_next_hour")]
    feature_cols = [c for c in df_model.columns if c not in target_cols + ["Fecha", "Hora", "datetime"]]
    station_cols = [c for c in feature_cols if c.startswith("Estacion_")]
    mediana = df_model[feature_cols].median(numeric_only=True)
    return {
        "feature_cols": feature_cols,
        "station_cols": station_cols,
        "median_features": mediana,
    }

def construir_vector_global(hora, mes, dia_num, estacion_txt, plantilla):
    fila = plantilla["median_features"].copy()
    fila.loc["hour"] = hora
    fila.loc["month"] = mes
    fila.loc["dayofweek"] = dia_num
    fila.loc["is_weekend"] = 1 if dia_num >= 5 else 0
    for col in plantilla["station_cols"]:
        fila.loc[col] = 0
    dummy = f"Estacion_{estacion_txt}"
    if dummy in fila.index:
        fila.loc[dummy] = 1
    vector = pd.DataFrame([fila])[plantilla["feature_cols"]]
    return vector

# Cargar recursos
df_metrics, mapa_estaciones = cargar_datos_csv()
dict_modelos = cargar_diccionario_modelos()
df_global_metrics = cargar_metricas_globales()
dict_modelos_globales = cargar_modelos_globales()
plantilla_global = cargar_plantilla_global()

st.title("☁️ Sistema de Predicción de Calidad de Aire")
st.markdown("Plataforma de Estimación de Contaminantes Atmosféricos en Lima usando Modelos Ensemble Learning y Random Forests")

tab1, tab2, tab3 = st.tabs(["📊 Comparativa de Rendimiento", "🤖 Simulador Interactivo", "📊 Aplicacion del capitulo Cap. 7: Ensemble Learning, Calidad de Aire"])

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

        st.markdown("### ⚙️ Configuración de la Simulación")
        lista_modelos = list(dict_modelos.keys())
        idx_default = 0
        if df_metrics is not None:
            mejor_nombre = df_metrics.sort_values("R2", ascending=False).iloc[0]['Modelo']
            if mejor_nombre in lista_modelos:
                idx_default = lista_modelos.index(mejor_nombre)

        modelo_seleccionado_nombre = st.selectbox("Seleccionar Algoritmo Predictivo:", lista_modelos, index=idx_default)
        modelo_activo = dict_modelos[modelo_seleccionado_nombre]

        st.markdown("---")

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            hora = st.slider("Hora del Día (0-23h)", 0, 23, 12)
        with c2:
            mes = st.selectbox("Mes del Año", range(1, 13), index=9, format_func=lambda x: f"Mes {x}")
        with c3:
            dias_txt = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            dia_txt = st.selectbox("Día de la Semana", dias_txt)
            dia_num = dias_txt.index(dia_txt)
        with c4:
            nombres_estaciones = list(mapa_estaciones.keys())
            estacion_txt = st.selectbox("Estación de Monitoreo", nombres_estaciones)
            estacion_code = mapa_estaciones[estacion_txt]

        if st.button("Ejecutar Predicción", type="primary"):
            entrada = np.array([[hora, mes, dia_num, estacion_code]])

            try:
                pred = modelo_activo.predict(entrada)[0]
                targets = ['PM 2.5', 'PM 10', 'SO2', 'NO2', 'O3', 'CO']

                st.subheader(f"Resultados con: {modelo_seleccionado_nombre}")

                cols = st.columns(6)
                for col, val, name in zip(cols, pred, targets):
                    col.metric(name, f"{val:.2f}", "µg/m³")

                st.caption(f"Perfil de contaminación estimado para {estacion_txt} a las {hora}:00")
                chart_data = pd.DataFrame({'Concentración': pred}, index=targets)
                st.bar_chart(chart_data)

            except Exception as e:
                st.error(f"Error técnico: {e}")
                st.info("Intenta volver a entrenar los modelos si cambiaste la estructura de datos.")

    else:
        st.warning("⚠️ Faltan archivos del sistema. Ejecuta 'entrenamiento_modelos.py' para generar los modelos.")

# --- PESTAÑA 3: APLICACION CAP. 7 ---
with tab3:
    st.subheader("Comparativa de rendimiento (modelos globales)")
    if df_global_metrics is not None and not df_global_metrics.empty:
        contaminantes = sorted(df_global_metrics["Contaminante"].unique())
        cont_sel = st.selectbox("Contaminante", contaminantes)
        df_sel = df_global_metrics[df_global_metrics["Contaminante"] == cont_sel].sort_values("r2", ascending=False)

        col_a, col_b = st.columns([1, 2])
        with col_a:
            st.dataframe(df_sel[["Modelo", "mae", "rmse", "r2"]], use_container_width=True)
        with col_b:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.barh(df_sel["Modelo"], df_sel["r2"], color="#4c72b0")
            ax.set_xlabel("R2 Score")
            ax.set_xlim(df_sel["r2"].min() - 0.05, 1.0)
            ax.grid(axis='x', linestyle='--', alpha=0.5)
            st.pyplot(fig)
    else:
        st.info("No se encontraron métricas globales. Ejecuta 'modelo_global.py' para generarlas.")

    st.markdown("---")
    st.subheader("Simulador interactivo (modelos globales)")
    if dict_modelos_globales and plantilla_global:
        algoritmos_disp = sorted(dict_modelos_globales.keys())
        estaciones_dummy = [c.replace("Estacion_", "") for c in plantilla_global["station_cols"]]
        if not estaciones_dummy and mapa_estaciones:
            estaciones_dummy = list(mapa_estaciones.keys())

        c1, c2 = st.columns(2)
        with c1:
            algoritmo_sel = st.selectbox("Modelo global", algoritmos_disp)
        with c2:
            estacion_global = st.selectbox("Estación", estaciones_dummy)

        c3, c4, c5 = st.columns(3)
        with c3:
            hora_g = st.slider("Hora (0-23h)", 0, 23, 12, key="hora_global")
        with c4:
            mes_g = st.selectbox("Mes", range(1, 13), index=9, key="mes_global", format_func=lambda x: f"Mes {x}")
        with c5:
            dias_txt = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            dia_txt_g = st.selectbox("Día", dias_txt, key="dia_global")
            dia_num_g = dias_txt.index(dia_txt_g)

        if st.button("Ejecutar predicción global", key="pred_global", type="primary"):
            try:
                entrada_global = construir_vector_global(hora_g, mes_g, dia_num_g, estacion_global, plantilla_global)
                resultados = {}
                for target, modelo in dict_modelos_globales.get(algoritmo_sel, {}).items():
                    pred_val = modelo.predict(entrada_global)[0]
                    nombre_mostrar = target.replace("_next_hour", "").replace("PM2_5", "PM 2.5").replace("PM10", "PM 10")
                    resultados[nombre_mostrar] = pred_val

                if resultados:
                    st.success(f"Predicción realizada con {algoritmo_sel.upper()}")
                    cols = st.columns(len(resultados))
                    for (name, val), col in zip(resultados.items(), cols):
                        col.metric(name, f"{val:.2f}", "µg/m³")
                    chart_df = pd.DataFrame({"Concentración": resultados.values()}, index=resultados.keys())
                    st.bar_chart(chart_df)
                else:
                    st.warning("No se encontraron modelos para el algoritmo seleccionado.")
            except Exception as e:
                st.error(f"Ocurrió un error al predecir: {e}")
                st.info("Si los modelos cambiaron, vuelve a ejecutar 'modelo_global.py' para regenerarlos.")
    else:
        st.warning("No hay modelos globales disponibles. Ejecuta 'modelo_global.py' para entrenarlos y guardarlos en /models.")

