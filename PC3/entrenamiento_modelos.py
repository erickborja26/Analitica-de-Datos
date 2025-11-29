import pandas as pd
import numpy as np
import os
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import BaggingRegressor, RandomForestRegressor, ExtraTreesRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.multioutput import MultiOutputRegressor

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_DATOS = os.path.join(BASE_DIR, '..', 'PC1', 'senamhi_detalle_limpio.csv')
RUTA_MODELOS = os.path.join(BASE_DIR, 'models')
os.makedirs(RUTA_MODELOS, exist_ok=True)

print("--- INICIO DEL ENTRENAMIENTO (MODO: TODOS LOS MODELOS DISPONIBLES) ---")

# 1. CARGA DE DATOS
if os.path.exists(RUTA_DATOS):
    df = pd.read_csv(RUTA_DATOS)
else:
    url = 'https://github.com/erickborja26/Analitica-de-Datos/raw/refs/heads/main/PC1/senamhi_detalle_limpio.csv'
    df = pd.read_csv(url)

# 2. PREPROCESAMIENTO
TARGETS = ['PM 2.5', 'PM 10', 'SO2', 'NO2', 'O3', 'CO']

def procesar_hora(str_hora):
    try:
        return int(str_hora.split(':')[0])
    except:
        return np.nan

if 'Hora' in df.columns:
    df['Hora_Num'] = df['Hora'].apply(procesar_hora)

if 'Fecha' in df.columns:
    df['Fecha_Dt'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y', errors='coerce')
    df['Mes'] = df['Fecha_Dt'].dt.month
    df['Dia_Semana'] = df['Fecha_Dt'].dt.dayofweek

# Mapeo de Estaciones
if 'Estacion' in df.columns:
    codigos, nombres_unicos = pd.factorize(df['Estacion'])
    df['Estacion_Code'] = codigos
    mapa_estaciones = {nombre: codigo for codigo, nombre in enumerate(nombres_unicos)}
    joblib.dump(mapa_estaciones, os.path.join(RUTA_MODELOS, 'mapa_estaciones.pkl'))
    print(f"[INFO] Mapa de estaciones guardado.")

# --- FEATURES  ---
features = ['Hora_Num', 'Mes', 'Dia_Semana', 'Estacion_Code']

# Limpieza
df_clean = df.dropna(subset=TARGETS + features)
X = df_clean[features]
y = df_clean[TARGETS]

# Imputación
imputer = SimpleImputer(strategy='most_frequent')
X_imputed = imputer.fit_transform(X)
X_train, X_test, y_train, y_test = train_test_split(X_imputed, y, test_size=0.2, random_state=42)

# 3. DEFINICIÓN DE MODELOS
modelos = {
    "1. Decision Tree": DecisionTreeRegressor(random_state=42),
    "2. Bagging (Trees)": BaggingRegressor(DecisionTreeRegressor(), n_estimators=50, bootstrap=True, n_jobs=-1, random_state=42),
    "3. Pasting (Trees)": BaggingRegressor(DecisionTreeRegressor(), n_estimators=50, bootstrap=False, n_jobs=-1, random_state=42),
    "4. Bagging (KNN)": BaggingRegressor(KNeighborsRegressor(), n_estimators=10, n_jobs=-1, random_state=42),
    "5. RF Estandar": RandomForestRegressor(n_estimators=100, n_jobs=-1, random_state=42),
    "6. RF Profundo": RandomForestRegressor(n_estimators=200, max_depth=None, n_jobs=-1, random_state=42),
    "7. RF Regularizado": RandomForestRegressor(n_estimators=100, max_depth=10, n_jobs=-1, random_state=42),
    "8. Extra-Trees": ExtraTreesRegressor(n_estimators=100, n_jobs=-1, random_state=42),
    "9. ET Optimizado": ExtraTreesRegressor(n_estimators=200, min_samples_leaf=3, n_jobs=-1, random_state=42),
    "10. RF FeatureSel": RandomForestRegressor(n_estimators=100, max_features='log2', n_jobs=-1, random_state=42)
}

# 4. ENTRENAMIENTO Y GUARDADO DE TODOS
resultados = []
modelos_entrenados = {} # Diccionario para guardar los objetos reales

print(f"[INFO] Entrenando 10 modelos...")
for nombre, modelo in modelos.items():
    modelo.fit(X_train, y_train) # Entrena
    modelos_entrenados[nombre] = modelo # Guarda el objeto entrenado en el dict
    
    score = modelo.score(X_test, y_test)
    rmse = np.sqrt(mean_squared_error(y_test, modelo.predict(X_test)))
    resultados.append({"Modelo": nombre, "R2": score, "RMSE": rmse})
    print(f"   > {nombre} finalizado.")

# 5. EXPORTAR
# Guardamos métricas
pd.DataFrame(resultados).to_csv(os.path.join(RUTA_MODELOS, 'metricas_integrante_1.csv'), index=False)

# Guardamos TODOS los modelos en un solo archivo (Diccionario serializado)
path_pkl = os.path.join(RUTA_MODELOS, 'todos_modelos_p1.pkl')
joblib.dump(modelos_entrenados, path_pkl)

print("-" * 50)
print(f"Entrenamiento finalizado.")
print(f"Se han guardado los 10 modelos en: {path_pkl}")