import os
from pathlib import Path
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# === 1. Cargar variables del archivo .env ===
load_dotenv(Path(__file__).parent / "config.env")

# === 2. Conexión MySQL ===
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "senamhi")

# === 3. Ruta al CSV limpio generado por el scraper ===
CSV_PATH = Path(__file__).resolve().parents[1] / "PC1" / "senamhi_detalle_limpio.csv"

def connect():
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

def get_or_create_station_id(cur, name: str) -> int:
    """Busca el ID de la estación o la crea si no existe."""
    cur.execute("SELECT id FROM stations WHERE name=%s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO stations (name) VALUES (%s)", (name,))
    return cur.lastrowid

def main():
    if not CSV_PATH.exists():
        print(f"❌ No existe el archivo {CSV_PATH}")
        return

    # === 4. Leer CSV ===
    df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    needed = ["Estacion","Fecha","Hora","PM 2.5","PM 10","SO2","NO2","O3","CO"]
    for col in needed:
        if col not in df.columns:
            df[col] = ""

    # === 5. Crear columna timestamp ===
    df["ts"] = pd.to_datetime(df["Fecha"].str.strip() + " " + df["Hora"].str.strip(),
                              errors="coerce", dayfirst=False)

    # === 6. Convertir valores a float ===
    for c, tgt in [("PM 2.5","pm2_5"),("PM 10","pm10"),("SO2","so2"),
                   ("NO2","no2"),("O3","o3"),("CO","co")]:
        df[tgt] = pd.to_numeric(df[c].str.strip().replace({"": None}), errors="coerce")

    # Filtrar filas sin timestamp válido
    df = df[~df["ts"].isna()].copy()

    cn = connect()
    cur = cn.cursor()

    upsert_sql = """
    INSERT INTO measurements (station_id, ts, pm2_5, pm10, so2, no2, o3, co)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      pm2_5=VALUES(pm2_5),
      pm10 =VALUES(pm10),
      so2  =VALUES(so2),
      no2  =VALUES(no2),
      o3   =VALUES(o3),
      co   =VALUES(co);
    """

    count = 0
    cache_station = {}

    for _, r in df.iterrows():
        name = (r["Estacion"] or "").strip()
        if not name:
            continue

        if name not in cache_station:
            sid = get_or_create_station_id(cur, name)
            cache_station[name] = sid
        else:
            sid = cache_station[name]

        cur.execute(upsert_sql, (
            sid, r["ts"].to_pydatetime(),
            r["pm2_5"] if pd.notna(r["pm2_5"]) else None,
            r["pm10"]  if pd.notna(r["pm10"])  else None,
            r["so2"]   if pd.notna(r["so2"])   else None,
            r["no2"]   if pd.notna(r["no2"])   else None,
            r["o3"]    if pd.notna(r["o3"])    else None,
            r["co"]    if pd.notna(r["co"])    else None,
        ))
        count += 1

    cn.commit()
    cur.close()
    cn.close()

    print(f"✅ Subida completa: {count} filas procesadas")

if __name__ == "__main__":
    main()
