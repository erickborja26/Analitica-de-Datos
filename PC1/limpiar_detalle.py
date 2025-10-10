from pathlib import Path
import pandas as pd
import numpy as np
import re

BASE = Path(__file__).resolve().parent
INP = BASE / "senamhi_detalle.csv"
OUT = BASE / "senamhi_detalle_limpio.csv"

# nombres esperados y contaminantes
SCHEMA = ["Estacion","Fecha","Hora","PM 2,5","PM 10","SO2","NO2","O3","CO"]
POLS = ["PM 2.5","PM 10","SO2","NO2","O3","CO"]

def normalize_str(s):
    if isinstance(s, str):
        return s.strip().replace('"', '').replace("'", "")
    return s

def limpiar_valor(v):
    """
    Limpia casos como:
    - "1,929.70"  -> 1929.70
    - 2.723.37    -> 2723.37
    - '  15,32 '  -> 15.32
    """
    if not isinstance(v, str):
        return v
    val = v.strip().replace('"', '').replace("'", "")
    # eliminar comas de miles (solo las que separan miles, no decimales)
    # caso "1,929.70" -> "1929.70"
    val = re.sub(r'(?<=\d),(?=\d{3}(\.|$))', '', val)
    # si tiene dos puntos (caso 2.723.37) -> eliminar el primero
    if val.count('.') >= 2:
        parts = val.split('.')
        val = ''.join(parts[:-2]) + '.' + parts[-1]
    # si tiene coma como decimal (15,32) -> punto
    if ',' in val and val.count(',') == 1 and '.' not in val:
        val = val.replace(',', '.')
    return val

def to_float(v):
    try:
        return float(v)
    except:
        return np.nan

def main():
    if not INP.exists():
        print(f"No existe {INP}, nada que limpiar.")
        return

    df = pd.read_csv(INP, dtype=str, encoding="utf-8-sig")

    # asegurar columnas
    for c in SCHEMA:
        if c not in df.columns:
            df[c] = ""

    # Renombrar columna PM 2,5 → PM 2.5
    rename_dict = {c: c.replace("PM 2,5", "PM 2.5") for c in df.columns if "PM 2,5" in c}
    df = df.rename(columns=rename_dict)

    # trimming strings
    for c in ["Estacion","Fecha","Hora"]:
        df[c] = df[c].map(normalize_str)

    # limpiar contaminantes
    for c in POLS:
        if c in df.columns:
            df[c] = df[c].map(normalize_str).map(limpiar_valor).map(to_float)

    # quitar duplicados
    df = df.drop_duplicates(subset=["Estacion","Fecha","Hora"], keep="last")

    # eliminar filas sin datos numéricos válidos
    mask_all_nan = df[[c for c in POLS if c in df.columns]].isna().all(axis=1)
    df = df[~mask_all_nan].copy()

    # eliminar negativos
    for c in POLS:
        if c in df.columns:
            df.loc[df[c] < 0, c] = np.nan

    # ordenar
    df = df.sort_values(by=["Estacion","Fecha","Hora"]).reset_index(drop=True)

    # guardar
    df.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"Limpieza completa: {len(df)} filas -> {OUT}")

if __name__ == "__main__":
    main()
