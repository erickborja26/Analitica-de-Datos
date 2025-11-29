"""
Modelos globales adicionales (10) para predecir todos los contaminantes a +1h.

Incluye:
- AdaBoost con Stumps
- AdaBoost Profundo
- GBRT Estándar
- GBRT con Shrinkage
- GBRT con Early Stopping
- Stochastic Gradient Boosting
- Histogram-Based Gradient Boosting
- Voting Regressor (promedio simple)
- Stacking Nivel 1 (Linear Blender)
- Stacking Nivel 2 (Forest Blender)
"""

import pickle
from pathlib import Path

import pandas as pd
from modelo_global import cargar_y_preparar
from sklearn.ensemble import (
    AdaBoostRegressor,
    GradientBoostingRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
    StackingRegressor,
    VotingRegressor,
)
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor


def modelos_para_target():
    """Construye el diccionario de modelos base (se clonan por target)."""
    return {
        "ada_stumps": AdaBoostRegressor(
            estimator=DecisionTreeRegressor(max_depth=1, random_state=42),
            n_estimators=400,
            learning_rate=0.1,
            random_state=42,
        ),
        "ada_profundo": AdaBoostRegressor(
            estimator=DecisionTreeRegressor(max_depth=3, random_state=42),
            n_estimators=300,
            learning_rate=0.05,
            random_state=42,
        ),
        "gbr_estandar": GradientBoostingRegressor(random_state=42),
        "gbr_shrinkage": GradientBoostingRegressor(
            n_estimators=500, learning_rate=0.05, max_depth=3, random_state=42
        ),
        "gbr_early": GradientBoostingRegressor(
            n_estimators=400,
            learning_rate=0.05,
            max_depth=3,
            random_state=42,
            validation_fraction=0.1,
            n_iter_no_change=5,
            tol=1e-3,
        ),
        "gbr_stochastic": GradientBoostingRegressor(
            n_estimators=400,
            learning_rate=0.05,
            max_depth=3,
            subsample=0.7,
            random_state=42,
        ),
        "hist_gbrt": HistGradientBoostingRegressor(
            learning_rate=0.05, max_depth=8, random_state=42
        ),
    }


def entrenar_modelos(df_model: pd.DataFrame, models_dir: Path):
    target_cols = [c for c in df_model.columns if c.endswith("_next_hour")]
    cols_excluir_base = ["Fecha", "Hora", "datetime"] + target_cols

    metricas = []

    for target in target_cols:
        feature_cols = [c for c in df_model.columns if c not in cols_excluir_base]
        X = df_model[feature_cols]
        y = df_model[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, shuffle=False
        )

        base_models = modelos_para_target()

        # Voting simple (promedio de 3 modelos)
        voting = VotingRegressor(
            estimators=[
                ("gbr_estandar", base_models["gbr_estandar"]),
                ("gbr_shrinkage", base_models["gbr_shrinkage"]),
                ("hist_gbrt", base_models["hist_gbrt"]),
            ]
        )

        # Stacking nivel 1 (linear blender)
        stack_lin = StackingRegressor(
            estimators=[
                ("ada_stumps", base_models["ada_stumps"]),
                ("gbr_estandar", base_models["gbr_estandar"]),
                ("hist_gbrt", base_models["hist_gbrt"]),
            ],
            final_estimator=LinearRegression(),
            n_jobs=1,
        )

        # Stacking nivel 2 (forest blender)
        stack_forest = StackingRegressor(
            estimators=[
                ("gbr_shrinkage", base_models["gbr_shrinkage"]),
                ("gbr_early", base_models["gbr_early"]),
                ("gbr_stochastic", base_models["gbr_stochastic"]),
            ],
            final_estimator=RandomForestRegressor(
                n_estimators=200, min_samples_leaf=2, random_state=42, n_jobs=1
            ),
            n_jobs=1,
        )

        modelos = {
            **base_models,
            "voting_simple": voting,
            "stacking_nivel1": stack_lin,
            "stacking_nivel2": stack_forest,
        }

        for nombre, modelo in modelos.items():
            modelo.fit(X_train, y_train)
            y_pred = modelo.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            rmse = mean_squared_error(y_test, y_pred) ** 0.5
            r2 = r2_score(y_test, y_pred)

            metricas.append(
                {"target": target, "modelo": nombre, "mae": mae, "rmse": rmse, "r2": r2}
            )

            fname = models_dir / f"modelo_global_extra_{nombre}_{target}.pkl"
            with open(fname, "wb") as f:
                pickle.dump(modelo, f)
            print(f"[{target}] Modelo {nombre} guardado en {fname}")

    return pd.DataFrame(metricas)


def main():
    base_dir = Path(__file__).resolve().parent
    df_model = cargar_y_preparar(base_dir / "calidad_aire.csv")
    print("Dataset global listo:", df_model.shape)

    models_dir = base_dir / "models"
    models_dir.mkdir(exist_ok=True)

    metricas_df = entrenar_modelos(df_model, models_dir)

    metrics_path = models_dir / "metricas_globales_extra.csv"
    metricas_df.to_csv(metrics_path, index=False)
    print(f"\nMetricas extra guardadas en: {metrics_path}")
    print(metricas_df.head())


if __name__ == "__main__":
    main()
