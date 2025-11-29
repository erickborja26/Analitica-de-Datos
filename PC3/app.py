import os
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

# Configuracion visual
st.set_page_config(page_title="Monitor Aire Lima", layout="wide")

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, "models")
RUTA_METRICAS = os.path.join(MODELS_DIR, "metricas_integrante_1.csv")
RUTA_MODELOS_DICT = os.path.join(MODELS_DIR, "todos_modelos_p1.pkl")
RUTA_MAPA = os.path.join(MODELS_DIR, "mapa_estaciones.pkl")
RUTA_METRICAS_GLOBAL = os.path.join(MODELS_DIR, "metricas_globales.csv")
RUTA_METRICAS_GLOBAL_EXTRA = os.path.join(MODELS_DIR, "metricas_globales_extra.csv")
RUTA_CALIDAD_AIRE = os.path.join(BASE_DIR, "calidad_aire.csv")

# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------
@st.cache_data
def cargar_datos_csv():
    metricas = pd.read_csv(RUTA_METRICAS) if os.path.exists(RUTA_METRICAS) else None
    mapa = joblib.load(RUTA_MAPA) if os.path.exists(RUTA_MAPA) else None
    return metricas, mapa


@st.cache_resource
def cargar_diccionario_modelos():
    if os.path.exists(RUTA_MODELOS_DICT):
        return joblib.load(RUTA_MODELOS_DICT)
    return None


def _limpiar_target(val: str) -> str:
    base = val.replace("_next_hour", "")
    return base.replace("PM2_5", "PM 2.5").replace("PM10", "PM 10")


@st.cache_data
def cargar_metricas_globales(ruta_metricas: str):
    if not os.path.exists(ruta_metricas):
        return None
    df = pd.read_csv(ruta_metricas)
    df["Contaminante"] = df["target"].apply(_limpiar_target)
    df["Modelo"] = df["modelo"].str.upper()
    return df


POLS = ["PM2_5", "PM10", "SO2", "NO2", "O3", "CO"]

def _split_nombre_modelo(base: str):
    for pol in POLS:
        suf = f"_{pol}_next_hour"
        if base.endswith(suf):
            algoritmo = base[: -len(suf)]
            if algoritmo.endswith("_"):
                algoritmo = algoritmo[:-1]
            return algoritmo, f"{pol}_next_hour"
    return None, None


@st.cache_resource
def cargar_modelos_globales(es_extra: bool = False):
    modelos = {}
    if not os.path.exists(MODELS_DIR):
        return modelos
    prefijo = "modelo_global_extra_" if es_extra else "modelo_global_"
    for fname in os.listdir(MODELS_DIR):
        if not fname.startswith(prefijo) or not fname.endswith(".pkl"):
            continue
        if not es_extra and fname.startswith("modelo_global_extra_"):
            continue
        base = fname[len(prefijo):-4]
        algoritmo, target = _split_nombre_modelo(base)
        if not algoritmo or not target:
            continue
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

# ---------------------------------------------------------------------------
# Cargar recursos
# ---------------------------------------------------------------------------
df_metrics, mapa_estaciones = cargar_datos_csv()
dict_modelos = cargar_diccionario_modelos()
df_global_metrics = cargar_metricas_globales(RUTA_METRICAS_GLOBAL)
df_global_metrics_extra = cargar_metricas_globales(RUTA_METRICAS_GLOBAL_EXTRA)
dict_modelos_globales = cargar_modelos_globales(es_extra=False)
dict_modelos_globales_extra = cargar_modelos_globales(es_extra=True)
plantilla_global = cargar_plantilla_global()

st.title("☁️ Sistema de Prediccion de Calidad de Aire")
st.markdown("Plataforma de estimacion de contaminantes atmosfericos en Lima usando modelos ensemble learning y random forests")

TAB_LABELS = [
    "📊 Comparativa de Rendimiento",
    "🤖 Simulador Interactivo",
    "📊Cap. 7: Ensemble Global",
    "📊 Modelos Extra",
]
tab1, tab2, tab3, tab4 = st.tabs(TAB_LABELS)

# ---------------------------------------------------------------------------
# PESTANA 1: RESULTADOS
# ---------------------------------------------------------------------------
with tab1:
    if df_metrics is not None:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("Ranking de Modelos")
            df_sorted = df_metrics.sort_values("R2", ascending=False)
            st.dataframe(df_sorted.style.highlight_max(subset=["R2"]), use_container_width=True)
            ganador = df_sorted.iloc[0]
            st.success(f"Mejor Modelo Global: **{ganador['Modelo']}**")
        with col2:
            st.subheader("Metrica R2 (Precision)")
            fig, ax = plt.subplots(figsize=(8, 4))
            colors = ["#2ca02c" if x == ganador["Modelo"] else "#4c72b0" for x in df_sorted["Modelo"]]
            ax.barh(df_sorted["Modelo"], df_sorted["R2"], color=colors)
            ax.set_xlabel("R2 Score (1.0 = Perfecto)")
            ax.set_xlim(0, 1.0)
            ax.grid(axis="x", linestyle="--", alpha=0.5)
            st.pyplot(fig)
    else:
        st.error("No se encontraron metricas. Ejecuta 'entrenamiento_modelos.py' primero.")

# ---------------------------------------------------------------------------
# PESTANA 2: SIMULADOR MULTI-OUTPUT
# ---------------------------------------------------------------------------
with tab2:
    if dict_modelos and mapa_estaciones:
        st.markdown("### ⚙️ Configuracion de la Simulacion")
        lista_modelos = list(dict_modelos.keys())
        idx_default = 0
        if df_metrics is not None:
            mejor_nombre = df_metrics.sort_values("R2", ascending=False).iloc[0]["Modelo"]
            if mejor_nombre in lista_modelos:
                idx_default = lista_modelos.index(mejor_nombre)
        modelo_seleccionado_nombre = st.selectbox("Seleccionar Algoritmo Predictivo:", lista_modelos, index=idx_default)
        modelo_activo = dict_modelos[modelo_seleccionado_nombre]

        st.markdown("---")
        c1, c2, c3, c4 = st.columns(4)
        dias_txt = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
        with c1:
            hora = st.slider("Hora del Dia (0-23h)", 0, 23, 12)
        with c2:
            mes = st.selectbox("Mes del Ano", range(1, 13), index=9, format_func=lambda x: f"Mes {x}")
        with c3:
            dia_txt = st.selectbox("Dia de la Semana", dias_txt)
            dia_num = dias_txt.index(dia_txt)
        with c4:
            nombres_estaciones = list(mapa_estaciones.keys())
            estacion_txt = st.selectbox("Estacion de Monitoreo", nombres_estaciones)
            estacion_code = mapa_estaciones[estacion_txt]

        if st.button("Ejecutar Prediccion", type="primary"):
            entrada = np.array([[hora, mes, dia_num, estacion_code]])
            try:
                pred = modelo_activo.predict(entrada)[0]
                targets = ["PM 2.5", "PM 10", "SO2", "NO2", "O3", "CO"]
                st.subheader(f"Resultados con: {modelo_seleccionado_nombre}")
                cols = st.columns(6)
                for col, val, name in zip(cols, pred, targets):
                    col.metric(name, f"{val:.2f}", "ug/m3")
                st.caption(f"Perfil de contaminacion estimado para {estacion_txt} a las {hora}:00")
                chart_data = pd.DataFrame({"Concentracion": pred}, index=targets)
                st.bar_chart(chart_data)
            except Exception as e:
                st.error(f"Error tecnico: {e}")
                st.info("Intenta volver a entrenar los modelos si cambiaste la estructura de datos.")
    else:
        st.warning("⚠️ Faltan archivos del sistema. Ejecuta 'entrenamiento_modelos.py' para generar los modelos.")

# ---------------------------------------------------------------------------
# Helpers de UI para tabs globales
# ---------------------------------------------------------------------------
def render_metricas(df_sel):
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.dataframe(df_sel[["Modelo", "mae", "rmse", "r2"]], use_container_width=True)
    with col_b:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.barh(df_sel["Modelo"], df_sel["r2"], color="#4c72b0")
        ax.set_xlabel("R2 Score")
        ax.set_xlim(df_sel["r2"].min() - 0.05, 1.0)
        ax.grid(axis="x", linestyle="--", alpha=0.5)
        st.pyplot(fig)


def render_simulador_global(modelos_dict, plantilla, estaciones_dummy, boton_key):
    algoritmos_disp = sorted(modelos_dict.keys())
    dias_txt = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    c1, c2 = st.columns(2)
    with c1:
        algoritmo_sel = st.selectbox("Modelo global", algoritmos_disp, key=f"alg_{boton_key}")
    with c2:
        estacion_global = st.selectbox("Estacion", estaciones_dummy, key=f"est_{boton_key}")

    c3, c4, c5 = st.columns(3)
    with c3:
        hora_g = st.slider("Hora (0-23h)", 0, 23, 12, key=f"hora_{boton_key}")
    with c4:
        mes_g = st.selectbox("Mes", range(1, 13), index=9, key=f"mes_{boton_key}", format_func=lambda x: f"Mes {x}")
    with c5:
        dia_txt_g = st.selectbox("Dia", dias_txt, key=f"dia_{boton_key}")
        dia_num_g = dias_txt.index(dia_txt_g)

    if st.button("Ejecutar prediccion", key=boton_key, type="primary"):
        try:
            entrada_global = construir_vector_global(hora_g, mes_g, dia_num_g, estacion_global, plantilla)
            resultados = {}
            for target, modelo in modelos_dict.get(algoritmo_sel, {}).items():
                pred_val = modelo.predict(entrada_global)[0]
                nombre_mostrar = _limpiar_target(target)
                resultados[nombre_mostrar] = pred_val
            if resultados:
                st.success(f"Prediccion realizada con {algoritmo_sel.upper()}")
                cols = st.columns(len(resultados))
                for (name, val), col in zip(resultados.items(), cols):
                    col.metric(name, f"{val:.2f}", "ug/m3")
                chart_df = pd.DataFrame({"Concentracion": resultados.values()}, index=resultados.keys())
                st.bar_chart(chart_df)
            else:
                st.warning("No se encontraron modelos para el algoritmo seleccionado.")
        except Exception as e:
            st.error(f"Ocurrio un error al predecir: {e}")
            st.info("Si los modelos cambiaron, vuelve a ejecutar el script de entrenamiento correspondiente.")

# ---------------------------------------------------------------------------
# PESTANA 3: MODELOS GLOBALES BASE
# ---------------------------------------------------------------------------
with tab3:
    st.subheader("Comparativa de rendimiento (modelos globales base)")
    if df_global_metrics is not None and not df_global_metrics.empty:
        contaminantes = sorted(df_global_metrics["Contaminante"].unique())
        cont_sel = st.selectbox("Contaminante", contaminantes, key="cont_base")
        df_sel = df_global_metrics[df_global_metrics["Contaminante"] == cont_sel].sort_values("r2", ascending=False)
        render_metricas(df_sel)
    else:
        st.info("No se encontraron metricas globales. Ejecuta 'modelo_global.py' para generarlas.")

    st.markdown("---")
    st.subheader("Simulador interactivo (modelos globales base)")
    if dict_modelos_globales and plantilla_global:
        estaciones_dummy = [c.replace("Estacion_", "") for c in plantilla_global["station_cols"]]
        if not estaciones_dummy and mapa_estaciones:
            estaciones_dummy = list(mapa_estaciones.keys())
        render_simulador_global(dict_modelos_globales, plantilla_global, estaciones_dummy, boton_key="pred_global_base")
    else:
        st.warning("No hay modelos globales disponibles. Ejecuta 'modelo_global.py' para entrenarlos y guardarlos en /models.")

# ---------------------------------------------------------------------------
# PESTANA 4: MODELOS GLOBALES EXTRA
# ---------------------------------------------------------------------------
with tab4:
    st.subheader("Comparativa de rendimiento (modelos globales extra)")
    if df_global_metrics_extra is not None and not df_global_metrics_extra.empty:
        contaminantes_extra = sorted(df_global_metrics_extra["Contaminante"].unique())
        cont_sel_extra = st.selectbox("Contaminante", contaminantes_extra, key="cont_extra")
        df_sel_extra = df_global_metrics_extra[df_global_metrics_extra["Contaminante"] == cont_sel_extra].sort_values("r2", ascending=False)
        render_metricas(df_sel_extra)
    else:
        st.info("No se encontraron metricas globales extra. Ejecuta 'modelos_globales_extra.py' para generarlas.")

    st.markdown("---")
    st.subheader("Simulador interactivo (modelos globales extra)")
    if dict_modelos_globales_extra and plantilla_global:
        estaciones_dummy_extra = [c.replace("Estacion_", "") for c in plantilla_global["station_cols"]]
        if not estaciones_dummy_extra and mapa_estaciones:
            estaciones_dummy_extra = list(mapa_estaciones.keys())
        render_simulador_global(dict_modelos_globales_extra, plantilla_global, estaciones_dummy_extra, boton_key="pred_global_extra")
    else:
        st.warning("No hay modelos globales extra disponibles. Ejecuta 'modelos_globales_extra.py' para entrenarlos y guardarlos en /models.")
