"""
Modelo global para predecir contaminantes (PM2.5, PM10, SO2, NO2, O3, CO) una hora adelante combinando todas las estaciones.

- Se unifican todas las estaciones con codificacion one-hot de Estacion.
- Se imputan faltantes por estacion con la mediana de cada columna (fallback mediana global)
  para rescatar estaciones con pocos datos.
- Se generan variables temporales, lags y target por contaminante, respetando el orden
  temporal por estacion.
- Se entrenan modelos globales (RF, GBR, XGB, Stacking) por cada contaminante.
"""

from pathlib import Path
import pickle

import pandas as pd
from sklearn.ensemble import (
    GradientBoostingRegressor,
    RandomForestRegressor,
    StackingRegressor,
)
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit, train_test_split
from xgboost import XGBRegressor


def cargar_y_preparar(ruta_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(ruta_csv)
    df = df.rename(columns={"PM 2.5": "PM2_5", "PM 10": "PM10"})
    df["datetime"] = pd.to_datetime(df["Fecha"] + " " + df["Hora"], format="%d/%m/%Y %H:%M")
    df = df.sort_values(["Estacion", "datetime"]).reset_index(drop=True)

    base_cols = ["Estacion", "Fecha", "Hora", "datetime"]
    pollutant_cols = [c for c in df.columns if c not in base_cols]

    # Elimina columnas que son todo NaN
    pollutant_cols = [c for c in pollutant_cols if df[c].notna().any()]
    df = df[base_cols + pollutant_cols]

    # Imputacion por estacion con mediana, luego fallback a mediana global
    for col in pollutant_cols:
        df[col] = df.groupby("Estacion")[col].transform(
            lambda s: s.fillna(s.median())
        )
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())

    # Variables temporales
    df["hour"] = df["datetime"].dt.hour
    df["dayofweek"] = df["datetime"].dt.dayofweek
    df["month"] = df["datetime"].dt.month
    df["is_weekend"] = (df["dayofweek"] >= 5).astype(int)

    # Lags y target dentro de cada estacion para no mezclar señales
    target_cols = []
    lag_cols = []
    for col in pollutant_cols:
        df[f"{col}_lag1"] = df.groupby("Estacion")[col].shift(1)
        df[f"{col}_lag2"] = df.groupby("Estacion")[col].shift(2)
        df[f"{col}_lag3"] = df.groupby("Estacion")[col].shift(3)
        df[f"{col}_next_hour"] = df.groupby("Estacion")[col].shift(-1)
        target_cols.append(f"{col}_next_hour")
        lag_cols.extend([f"{col}_lag1", f"{col}_lag2", f"{col}_lag3"])

    # Eliminamos filas con NaN en lags/targets (solo por los que existan)
    df = df.dropna(subset=lag_cols + target_cols)

    # One-hot de Estacion
    df = pd.get_dummies(df, columns=["Estacion"], drop_first=True)
    return df


def entrenar_modelos(df_model: pd.DataFrame):
    # Todas las columnas target disponibles (terminan en _next_hour)
    target_cols = [c for c in df_model.columns if c.endswith("_next_hour")]

    # Columnas que nunca son features
    cols_excluir_base = ["Fecha", "Hora", "datetime"] + target_cols

    resultados = {}
    modelos = {"rf": {}, "gbr": {}, "xgb": {}, "stack": {}}
    filas_metricas = []

    for target in target_cols:
        cols_excluir = cols_excluir_base.copy()
        # Features: todo excepto targets y columnas base
        feature_cols = [c for c in df_model.columns if c not in cols_excluir]

        X = df_model[feature_cols]
        y = df_model[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, shuffle=False
        )

        n_splits = min(3, len(X_train) - 1)
        tscv = TimeSeriesSplit(n_splits=n_splits) if n_splits >= 2 else None

        # RandomForest con grid
        rf_base = RandomForestRegressor(
            n_estimators=300, max_depth=None, min_samples_leaf=2, random_state=42, n_jobs=1
        )
        rf_base.fit(X_train, y_train)
        y_pred = rf_base.predict(X_test)
        best_rf = rf_base
        res_rf = (
            mean_absolute_error(y_test, y_pred),
            mean_squared_error(y_test, y_pred) ** 0.5,
            r2_score(y_test, y_pred),
        )
        if tscv:
            grid_rf = GridSearchCV(
                RandomForestRegressor(random_state=42, n_jobs=1),
                param_grid={
                    "n_estimators": [200, 300],
                    "max_depth": [None, 10],
                    "min_samples_leaf": [1, 2],
                },
                cv=tscv,
                scoring="neg_mean_absolute_error",
                n_jobs=1,
            )
            grid_rf.fit(X_train, y_train)
            best_rf = grid_rf.best_estimator_
            y_pred = best_rf.predict(X_test)
            res_rf = (
                mean_absolute_error(y_test, y_pred),
                mean_squared_error(y_test, y_pred) ** 0.5,
                r2_score(y_test, y_pred),
            )
            print(f"[{target}] Grid RF mejores params:", grid_rf.best_params_)

        # GradientBoosting con grid
        gbr_base = GradientBoostingRegressor(
            n_estimators=300, learning_rate=0.05, max_depth=3, random_state=42
        )
        gbr_base.fit(X_train, y_train)
        y_pred = gbr_base.predict(X_test)
        best_gbr = gbr_base
        res_gbr = (
            mean_absolute_error(y_test, y_pred),
            mean_squared_error(y_test, y_pred) ** 0.5,
            r2_score(y_test, y_pred),
        )
        if tscv:
            grid_gbr = GridSearchCV(
                GradientBoostingRegressor(random_state=42),
                param_grid={
                    "n_estimators": [200, 300],
                    "learning_rate": [0.05, 0.1],
                    "max_depth": [2, 3],
                    "subsample": [0.8, 1.0],
                },
                cv=tscv,
                scoring="neg_mean_absolute_error",
                n_jobs=1,
            )
            grid_gbr.fit(X_train, y_train)
            best_gbr = grid_gbr.best_estimator_
            y_pred = best_gbr.predict(X_test)
            res_gbr = (
                mean_absolute_error(y_test, y_pred),
                mean_squared_error(y_test, y_pred) ** 0.5,
                r2_score(y_test, y_pred),
            )
            print(f"[{target}] Grid GBR mejores params:", grid_gbr.best_params_)

        # XGBoost con grid
        xgb_base = XGBRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4,
            subsample=0.9,
            colsample_bytree=0.9,
            objective="reg:squarederror",
            random_state=42,
            n_jobs=1,
            tree_method="hist",
        )
        xgb_base.fit(X_train, y_train)
        y_pred = xgb_base.predict(X_test)
        best_xgb = xgb_base
        res_xgb = (
            mean_absolute_error(y_test, y_pred),
            mean_squared_error(y_test, y_pred) ** 0.5,
            r2_score(y_test, y_pred),
        )
        if tscv:
            grid_xgb = GridSearchCV(
                XGBRegressor(
                    objective="reg:squarederror",
                    random_state=42,
                    n_jobs=1,
                    tree_method="hist",
                ),
                param_grid={
                    "n_estimators": [200, 300],
                    "learning_rate": [0.05, 0.1],
                    "max_depth": [3, 4],
                    "subsample": [0.8, 1.0],
                    "colsample_bytree": [0.8, 1.0],
                },
                cv=tscv,
                scoring="neg_mean_absolute_error",
                n_jobs=1,
            )
            grid_xgb.fit(X_train, y_train)
            best_xgb = grid_xgb.best_estimator_
            y_pred = best_xgb.predict(X_test)
            res_xgb = (
                mean_absolute_error(y_test, y_pred),
                mean_squared_error(y_test, y_pred) ** 0.5,
                r2_score(y_test, y_pred),
            )
            print(f"[{target}] Grid XGB mejores params:", grid_xgb.best_params_)

        stack = StackingRegressor(
            estimators=[("rf", best_rf), ("gbr", best_gbr), ("xgb", best_xgb)],
            final_estimator=LinearRegression(),
            n_jobs=1,
        )
        stack.fit(X_train, y_train)
        y_pred = stack.predict(X_test)
        res_stack = (
            mean_absolute_error(y_test, y_pred),
            mean_squared_error(y_test, y_pred) ** 0.5,
            r2_score(y_test, y_pred),
        )

        resultados[target] = {
            "rf": res_rf,
            "gbr": res_gbr,
            "xgb": res_xgb,
            "stack": res_stack,
        }

        for nombre, (mae, rmse, r2) in resultados[target].items():
            filas_metricas.append(
                {
                    "target": target,
                    "modelo": nombre,
                    "mae": mae,
                    "rmse": rmse,
                    "r2": r2,
                }
            )

        modelos["rf"][target] = best_rf
        modelos["gbr"][target] = best_gbr
        modelos["xgb"][target] = best_xgb
        modelos["stack"][target] = stack

    return resultados, modelos, filas_metricas


def main():
    df_model = cargar_y_preparar(Path("calidad_aire.csv"))
    print("Dataset global listo:", df_model.shape)

    models_dir = Path("models")
    models_dir.mkdir(exist_ok=True)

    resultados, modelos, filas_metricas = entrenar_modelos(df_model)

    print("\n=== Metricas globales por contaminante ===")
    for target, res in resultados.items():
        print(f"\nTarget: {target}")
        for nombre, (mae, rmse, r2) in res.items():
            print(f"  {nombre.upper():6s} -> MAE: {mae:.3f} | RMSE: {rmse:.3f} | R2: {r2:.3f}")

    # Guarda todos los modelos por contaminante
    for nombre_modelo, targets in modelos.items():
        for target, modelo in targets.items():
            fname = models_dir / f"modelo_global_{nombre_modelo}_{target}.pkl"
            with open(fname, "wb") as f:
                pickle.dump(modelo, f)
            print(f"Modelo {nombre_modelo.upper()} guardado: {fname}")

    # Guarda métricas en CSV
    metricas_df = pd.DataFrame(filas_metricas)
    metricas_csv = models_dir / "metricas_globales.csv"
    metricas_df.to_csv(metricas_csv, index=False)
    print(f"\nMetricas guardadas en: {metricas_csv}")


if __name__ == "__main__":
    main()
