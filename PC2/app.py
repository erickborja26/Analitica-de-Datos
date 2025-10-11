# PC2/app.py
from __future__ import annotations
import os, csv, io
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Tuple

from flask import Flask, jsonify, request, Response, abort
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

# ---------------------------
# Config & bootstrap
# ---------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "config.env"))

DB_CFG = dict(
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "senamhi"),
    charset="utf8mb4",
    autocommit=False,
)

API_KEY = os.getenv("API_KEY")  # si None, no se valida
DEFAULT_TZ = os.getenv("DEFAULT_TZ", "America/Lima")

POOL: pooling.MySQLConnectionPool | None = None


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    # connection pool
    global POOL
    POOL = pooling.MySQLConnectionPool(pool_name="senamhi_pool", pool_size=5, **DB_CFG)

    # --- helpers ---

    def get_conn():
        return POOL.get_connection()

    def require_api_key():
        if API_KEY:
            sent = request.headers.get("X-API-Key")
            if not sent or sent != API_KEY:
                abort(401, description="Unauthorized")

    def parse_limit_offset() -> Tuple[int, int]:
        def clamp(v, lo, hi, default):
            try:
                x = int(v)
                return max(lo, min(hi, x))
            except:
                return default
        limit = clamp(request.args.get("limit"), 1, 1000, 100)
        offset = clamp(request.args.get("offset"), 0, 10**9, 0)
        return limit, offset

    def parse_tz() -> ZoneInfo:
        tz_name = request.args.get("tz") or DEFAULT_TZ
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return ZoneInfo(DEFAULT_TZ)

    def to_iso(dt: datetime, tz: ZoneInfo) -> str:
        """
        Suponemos que en DB ts está en hora local Lima (naive).
        Lo tratamos como 'DEFAULT_TZ' y convertimos a la tz destino.
        """
        if dt.tzinfo is None:
            src = ZoneInfo(DEFAULT_TZ)
            dt = dt.replace(tzinfo=src)
        return dt.astimezone(tz).isoformat()

    def row_to_measurement_dict(row: Dict[str, Any], tz: ZoneInfo) -> Dict[str, Any]:
        # row keys from SQL must include: ts, pm2_5, pm10, so2, no2, o3, co
        out = {
            "ts": to_iso(row["ts"], tz),
            "pm25": row.get("pm2_5"),
            "pm10": row.get("pm10"),
            "so2":  row.get("so2"),
            "no2":  row.get("no2"),
            "o3":   row.get("o3"),
            "co":   row.get("co"),
        }
        return out

    def dict_rows(cursor) -> List[Dict[str, Any]]:
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, r)) for r in cursor.fetchall()]

    def build_fields_clause() -> Tuple[str, List[str]]:
        """
        fields=pm25,pm10  -> solo selecciona esas columnas.
        En DB las columnas son: pm2_5, pm10, so2, no2, o3, co
        """
        allowed = {
            "pm25": "pm2_5",
            "pm10": "pm10",
            "so2": "so2",
            "no2": "no2",
            "o3": "o3",
            "co": "co",
        }
        fields_param = request.args.get("fields")
        if not fields_param:
            return "m.ts, m.pm2_5, m.pm10, m.so2, m.no2, m.o3, m.co", list(allowed.keys())

        req = [f.strip().lower() for f in fields_param.split(",") if f.strip()]
        db_cols = []
        for f in req:
            if f in allowed:
                db_cols.append(allowed[f])
        if not db_cols:
            return "m.ts", []
        select = ", ".join(["m.ts"] + [f"m.{c}" for c in db_cols])
        return select, req

    # ==== ALERTAS: helpers y endpoints ====

    POLLUTANT_DB_COL = {
        "pm25": "pm2_5",
        "pm10": "pm10",
        "so2":  "so2",
        "no2":  "no2",
        "o3":   "o3",
        "co":   "co",
    }

    VALID_OPERATORS = {"gt", "ge", "lt", "le"}

    def _compare(val: float | None, op: str, thr: float) -> bool:
        if val is None:
            return False
        if op == "gt": return val >  thr
        if op == "ge": return val >= thr
        if op == "lt": return val <  thr
        if op == "le": return val <= thr
        return False

    def _fetch_latest_by_station(cur, station_ids: list[int] | None = None) -> dict[int, dict]:
        """
        Devuelve {station_id: { 'ts': datetime, 'pm2_5':..., 'pm10':... }} con la última medición por estación.
        Si station_ids es None => todas las estaciones.
        """
        if station_ids:
            cur.execute(
                """
                SELECT m.*
                FROM measurements m
                JOIN (
                    SELECT station_id, MAX(ts) AS mx FROM measurements
                    WHERE station_id IN (%s)
                    GROUP BY station_id
                ) t ON t.station_id=m.station_id AND t.mx=m.ts
                """ % (",".join(["%s"] * len(station_ids))),
                station_ids
            )
        else:
            cur.execute(
                """
                SELECT m.*
                FROM measurements m
                JOIN (
                    SELECT station_id, MAX(ts) AS mx FROM measurements GROUP BY station_id
                ) t ON t.station_id=m.station_id AND t.mx=m.ts
                """
            )
        rows = cur.fetchall()
        by_station = { r["station_id"]: r for r in rows }
        return by_station

    def evaluate_rules(rule_id: int | None = None) -> int:
        """
        Evalúa reglas habilitadas (o una en particular) contra la última medición por estación
        y genera alert_events (sin duplicar) cuando se cumple la condición.
        Retorna cantidad de eventos insertados (nuevos).
        """
        inserted = 0
        with POOL.get_connection() as cn, cn.cursor(dictionary=True) as cur:
            # 1) Carga reglas habilitadas
            if rule_id:
                cur.execute("SELECT * FROM alert_rules WHERE enabled=1 AND id=%s", (rule_id,))
            else:
                cur.execute("SELECT * FROM alert_rules WHERE enabled=1")
            rules = cur.fetchall()
            if not rules:
                return 0

            # 2) Evalúa
            for rule in rules:
                pollutant = (rule["pollutant"] or "").lower()
                op = (rule["operator"] or "").lower()
                thr = float(rule["threshold"])
                if pollutant not in POLLUTANT_DB_COL or op not in VALID_OPERATORS:
                    continue
                col = POLLUTANT_DB_COL[pollutant]

                # estaciones objetivo
                if rule["station_id"]:
                    stations = [rule["station_id"]]
                else:
                    # todas
                    cur.execute("SELECT id FROM stations")
                    stations = [r["id"] for r in cur.fetchall()]

                # Últimas mediciones por estación (solo las involucradas)
                latest = _fetch_latest_by_station(cur, stations)

                # 3) Inserta eventos cuando corresponda
                for sid in stations:
                    meas = latest.get(sid)
                    if not meas:
                        continue
                    ts = meas["ts"]
                    val = meas.get(col)

                    if _compare(val, op, thr):
                        # inserta con upsert para no duplicar
                        cur.execute(
                            """
                            INSERT INTO alert_events
                              (rule_id, station_id, ts, pollutant, value, operator, threshold)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                              value=VALUES(value), operator=VALUES(operator), threshold=VALUES(threshold)
                            """,
                            (rule["id"], sid, ts, pollutant, val, op, thr)
                        )
                        # rowcount será 1 si insertó, 2 si actualizó; contamos solo insert nuevos
                        if cur.rowcount == 1:
                            inserted += 1
            cn.commit()
        return inserted

    # ---------- Endpoints de reglas ----------

    @app.route("/v1/alerts/rules", methods=["GET"])
    def list_rules():
        with POOL.get_connection() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT r.id, r.name, r.station_id, s.name AS station_name,
                       r.pollutant, r.operator, r.threshold, r.time_window AS window, r.enabled, r.created_at
                FROM alert_rules r
                LEFT JOIN stations s ON s.id=r.station_id
                ORDER BY r.created_at DESC
            """)
            rules = cur.fetchall()
        return jsonify({"items": rules})

    @app.route("/v1/alerts/rules", methods=["POST"])
    def create_rule():
        data = request.get_json(force=True, silent=True) or {}
        name = (data.get("name") or "").strip()
        station_id = data.get("station_id")  # None => todas
        pollutant = (data.get("pollutant") or "").lower()
        operator = (data.get("operator") or "").lower()
        threshold = data.get("threshold")
        window = data.get("window")  # opcional
        enabled = bool(data.get("enabled", True))

        # Validación
        if not name:
            abort(400, description="name is required")
        if pollutant not in POLLUTANT_DB_COL:
            abort(400, description="pollutant must be one of: " + ",".join(POLLUTANT_DB_COL.keys()))
        if operator not in VALID_OPERATORS:
            abort(400, description="operator must be one of: gt,ge,lt,le")
        try:
            threshold = float(threshold)
        except:
            abort(400, description="threshold must be numeric")
        if station_id is not None:
            try:
                station_id = int(station_id)
            except:
                abort(400, description="station_id must be integer or null")

        with POOL.get_connection() as cn, cn.cursor(dictionary=True) as cur:
            # si station_id viene, valida que exista
            if station_id is not None:
                cur.execute("SELECT id FROM stations WHERE id=%s", (station_id,))
                if not cur.fetchone():
                    abort(400, description="station_id not found")

            cur.execute(
                """INSERT INTO alert_rules
                   (name, station_id, pollutant, operator, threshold, time_window, enabled)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (name, station_id, pollutant, operator, threshold, window, 1 if enabled else 0)
            )
            rid = cur.lastrowid
            cn.commit()

        # Evaluación inmediata opcional ?evaluate_now=true
        if (request.args.get("evaluate_now") or "").lower() in {"1", "true", "yes"}:
            created = evaluate_rules(rule_id=rid)
            return jsonify({"created_id": rid, "evaluated_events_created": created}), 201

        return jsonify({"created_id": rid}), 201

    @app.route("/v1/alerts/rules/<int:rule_id>", methods=["PUT"])
    def update_rule(rule_id: int):
        data = request.get_json(force=True, silent=True) or {}
        fields = []
        params = []

        if "name" in data:
            fields.append("name=%s"); params.append((data["name"] or "").strip())
        if "station_id" in data:
            sid = data["station_id"]
            if sid is not None:
                try: sid = int(sid)
                except: abort(400, description="station_id must be integer or null")
            fields.append("station_id=%s"); params.append(sid)
        if "pollutant" in data:
            pol = (data["pollutant"] or "").lower()
            if pol not in POLLUTANT_DB_COL: abort(400, description="invalid pollutant")
            fields.append("pollutant=%s"); params.append(pol)
        if "operator" in data:
            op = (data["operator"] or "").lower()
            if op not in VALID_OPERATORS: abort(400, description="invalid operator")
            fields.append("operator=%s"); params.append(op)
        if "threshold" in data:
            try: th = float(data["threshold"])
            except: abort(400, description="threshold must be numeric")
            fields.append("threshold=%s"); params.append(th)
        if "window" in data:
            fields.append("time_window=%s"); params.append(data["window"])
        if "enabled" in data:
            en = 1 if bool(data["enabled"]) else 0
            fields.append("enabled=%s"); params.append(en)

        if not fields:
            abort(400, description="no fields to update")

        params.append(rule_id)
        with POOL.get_connection() as cn, cn.cursor() as cur:
            cur.execute(f"UPDATE alert_rules SET {', '.join(fields)} WHERE id=%s", tuple(params))
            if cur.rowcount == 0:
                abort(404, description="rule not found")
            cn.commit()
        return jsonify({"updated_id": rule_id})

    @app.route("/v1/alerts/rules/<int:rule_id>", methods=["DELETE"])
    def delete_rule(rule_id: int):
        with POOL.get_connection() as cn, cn.cursor() as cur:
            cur.execute("DELETE FROM alert_rules WHERE id=%s", (rule_id,))
            if cur.rowcount == 0:
                abort(404, description="rule not found")
            cn.commit()
        return jsonify({"deleted_id": rule_id})

    # ---------- Evaluación manual (genera eventos) ----------

    @app.route("/v1/alerts/evaluate", methods=["POST"])
    def evaluate_alerts_endpoint():
        """
        Ejecuta la evaluación de TODAS las reglas habilitadas (o una rule_id si viene en el body).
        Retorna cuántos eventos nuevos se crearon.
        """
        data = request.get_json(force=True, silent=True) or {}
        rid = data.get("rule_id")
        if rid is not None:
          try: rid = int(rid)
          except: abort(400, description="rule_id must be integer")
        created = evaluate_rules(rule_id=rid)
        return jsonify({"events_created": created})

    # ---------- Listado de eventos ----------

    @app.route("/v1/alerts/events", methods=["GET"])
    def list_events():
        limit, offset =  parse_limit_offset()
        rid = request.args.get("rule_id")
        sid = request.args.get("station_id")
        start = request.args.get("start")
        end = request.args.get("end")

        where = []
        params: list = []

        if rid:
            try: rid = int(rid)
            except: abort(400, description="rule_id must be integer")
            where.append("e.rule_id=%s"); params.append(rid)

        if sid:
            try: sid = int(sid)
            except: abort(400, description="station_id must be integer")
            where.append("e.station_id=%s"); params.append(sid)

        # parse fechas (ISO) a local naive
        def parse_dt(x):
            if not x: return None
            try:
                dt = datetime.fromisoformat(x.replace("Z","+00:00"))
                local = dt.astimezone(ZoneInfo(DEFAULT_TZ)).replace(tzinfo=None)
                return local.strftime("%Y-%m-%d %H:%M:%S")
            except:
                return None

        p_start = parse_dt(start); p_end = parse_dt(end)
        if p_start: where.append("e.ts >= %s"); params.append(p_start)
        if p_end:   where.append("e.ts <= %s"); params.append(p_end)

        where_sql = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
          SELECT e.id, e.rule_id, r.name AS rule_name, e.station_id, s.name AS station_name,
                 e.ts, e.pollutant, e.value, e.operator, e.threshold, e.created_at
          FROM alert_events e
          JOIN alert_rules r ON r.id=e.rule_id
          JOIN stations s ON s.id=e.station_id
          {where_sql}
          ORDER BY e.ts DESC
          LIMIT %s OFFSET %s
        """

        params += [limit, offset]
        with POOL.get_connection() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        return jsonify({"items": rows, "limit": limit, "offset": offset})

    # ---------------------------
    # Routes
    # ---------------------------

    @app.route("/v1/health", methods=["GET"])
    def health():
        tz = parse_tz()
        try:
            with get_conn() as cn, cn.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
            return jsonify({"status": "ok", "db": "ok", "time": datetime.now(ZoneInfo(DEFAULT_TZ)).astimezone(tz).isoformat()})
        except Exception as e:
            return jsonify({"status": "degraded", "db": f"error: {e.__class__.__name__}"}), 500

    # ---------- Stations ----------

    @app.route("/v1/stations", methods=["GET"])
    def list_stations():
        # require_api_key()  # descomenta si quieres proteger
        q = request.args.get("q", "").strip()
        limit, offset = parse_limit_offset()
        where = []
        params: List[Any] = []
        if q:
            where.append("name LIKE %s")
            params.append(f"%{q}%")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"SELECT id, name FROM stations {where_sql} ORDER BY name ASC LIMIT %s OFFSET %s"
        count_sql = f"SELECT COUNT(*) FROM stations {where_sql}"
        with get_conn() as cn, cn.cursor() as cur:
            if where:
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()[0]
                cur.execute(sql, tuple(params + [limit, offset]))
            else:
                cur.execute(count_sql)
                total = cur.fetchone()[0]
                cur.execute(sql, (limit, offset))
            items = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
        return jsonify({"items": items, "total": total, "limit": limit, "offset": offset})

    @app.route("/v1/stations/<int:station_id>", methods=["GET"])
    def get_station(station_id: int):
        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, name FROM stations WHERE id=%s", (station_id,))
            row = cur.fetchone()
            if not row:
                abort(404, description="Station not found")
            return jsonify(row)

    # ---------- Latest per station ----------

    @app.route("/v1/stations/<int:station_id>/latest", methods=["GET"])
    def station_latest(station_id: int):
        tz = parse_tz()
        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, name FROM stations WHERE id=%s", (station_id,))
            st = cur.fetchone()
            if not st:
                abort(404, description="Station not found")
            cur.execute(
                """
                SELECT m.ts, m.pm2_5, m.pm10, m.so2, m.no2, m.o3, m.co
                FROM measurements m
                WHERE m.station_id=%s
                ORDER BY m.ts DESC
                LIMIT 1
                """,
                (station_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"station_id": station_id, "station_name": st["name"], "item": None})
            item = row_to_measurement_dict(row, tz)
            return jsonify({"station_id": station_id, "station_name": st["name"], "item": item})

    @app.route("/v1/measurements/latest", methods=["GET"])
    def latest_all():
        tz = parse_tz()
        limit, offset = parse_limit_offset()
        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            # Última por estación usando subconsulta
            cur.execute(
                """
                SELECT s.id AS station_id, s.name AS station_name,
                       m.ts, m.pm2_5, m.pm10, m.so2, m.no2, m.o3, m.co
                FROM stations s
                JOIN (
                    SELECT station_id, MAX(ts) AS max_ts
                    FROM measurements
                    GROUP BY station_id
                ) t ON t.station_id = s.id
                JOIN measurements m ON m.station_id = t.station_id AND m.ts = t.max_ts
                ORDER BY s.name ASC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
        items = []
        for r in rows:
            items.append({
                "station_id": r["station_id"],
                "station_name": r["station_name"],
                **row_to_measurement_dict(r, tz)
            })
        return jsonify({"items": items, "limit": limit, "offset": offset})

    # ---------- Range queries ----------

    @app.route("/v1/stations/<int:station_id>/measurements", methods=["GET"])
    def station_measurements(station_id: int):
        tz = parse_tz()
        limit, offset = parse_limit_offset()
        start = request.args.get("start")
        end = request.args.get("end")
        order = (request.args.get("order") or "asc").lower()
        order = "ASC" if order != "desc" else "DESC"

        select_clause, _ = build_fields_clause()

        where = ["m.station_id=%s"]
        params: List[Any] = [station_id]

        # parse start/end as ISO
        def parse_dt(x: Optional[str]) -> Optional[str]:
            if not x: return None
            try:
                # Acepta ISO con tz; lo convertimos a DEFAULT_TZ sin tzinfo
                dt = datetime.fromisoformat(x.replace("Z","+00:00"))
                # normaliza a DEFAULT_TZ naive
                local = dt.astimezone(ZoneInfo(DEFAULT_TZ)).replace(tzinfo=None)
                return local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return None

        p_start = parse_dt(start)
        p_end = parse_dt(end)
        if p_start:
            where.append("m.ts >= %s")
            params.append(p_start)
        if p_end:
            where.append("m.ts <= %s")
            params.append(p_end)

        where_sql = "WHERE " + " AND ".join(where)
        sql = f"""
            SELECT {select_clause}
            FROM measurements m
            {where_sql}
            ORDER BY m.ts {order}
            LIMIT %s OFFSET %s
        """
        params += [limit, offset]

        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, name FROM stations WHERE id=%s", (station_id,))
            st = cur.fetchone()
            if not st:
                abort(404, description="Station not found")
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        items = [row_to_measurement_dict(r, tz) for r in rows]
        return jsonify({"station": {"id": st["id"], "name": st["name"]}, "items": items, "limit": limit, "offset": offset})

    @app.route("/v1/measurements", methods=["GET"])
    def measurements_multi():
        tz = parse_tz()
        limit, offset = parse_limit_offset()
        order = (request.args.get("order") or "asc").lower()
        order = "ASC" if order != "desc" else "DESC"
        select_clause, _ = build_fields_clause()

        where = []
        params: List[Any] = []

        # station_id=1&station_id=2...
        station_ids = request.args.getlist("station_id")
        if station_ids:
            where.append("m.station_id IN (" + ",".join(["%s"] * len(station_ids)) + ")")
            params += [int(x) for x in station_ids]

        # station_name=foo,bar
        station_name_csv = request.args.get("station_name")
        if station_name_csv:
            names = [x.strip() for x in station_name_csv.split(",") if x.strip()]
            if names:
                where.append("s.name IN (" + ",".join(["%s"] * len(names)) + ")")
                params += names

        # start/end
        def parse_dt(x: Optional[str]) -> Optional[str]:
            if not x: return None
            try:
                dt = datetime.fromisoformat(x.replace("Z","+00:00"))
                local = dt.astimezone(ZoneInfo(DEFAULT_TZ)).replace(tzinfo=None)
                return local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
        p_start = parse_dt(request.args.get("start"))
        p_end = parse_dt(request.args.get("end"))
        if p_start:
            where.append("m.ts >= %s"); params.append(p_start)
        if p_end:
            where.append("m.ts <= %s"); params.append(p_end)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT s.id AS station_id, s.name AS station_name, {select_clause}
            FROM measurements m
            JOIN stations s ON s.id = m.station_id
            {where_sql}
            ORDER BY m.ts {order}
            LIMIT %s OFFSET %s
        """
        params += [limit, offset]

        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        items = []
        for r in rows:
            itm = {
                "station_id": r["station_id"],
                "station_name": r["station_name"],
                **row_to_measurement_dict(r, tz)
            }
            items.append(itm)
        return jsonify({"items": items, "limit": limit, "offset": offset})

    # ---------- Aggregates ----------

    def aggregate_sql(granularity: str, agg: str = "avg") -> str:
        """
        granularity: 'hourly'|'daily'
        agg: 'avg'|'median'|'max'|'min'  (implementamos avg y max/min; median se emula si quisieras)
        """
        bucket = "DATE_FORMAT(m.ts, '%Y-%m-%d %H:00:00')" if granularity == "hourly" else "DATE(m.ts)"
        fn = {"avg":"AVG","max":"MAX","min":"MIN"}.get(agg.lower(), "AVG")
        return f"""
            SELECT m.station_id,
                   {bucket} AS bucket,
                   {fn}(m.pm2_5) AS pm2_5,
                   {fn}(m.pm10)  AS pm10,
                   {fn}(m.so2)   AS so2,
                   {fn}(m.no2)   AS no2,
                   {fn}(m.o3)    AS o3,
                   {fn}(m.co)    AS co
            FROM measurements m
            WHERE m.station_id IN ({{station_ids}})
              {{time_filter}}
            GROUP BY m.station_id, bucket
            ORDER BY bucket ASC
        """

    def parse_station_ids_required() -> List[int]:
        ids = request.args.getlist("station_id")
        if not ids:
            abort(400, description="station_id is required (one or more).")
        try:
            return [int(x) for x in ids]
        except:
            abort(400, description="station_id must be integer.")

    @app.route("/v1/aggregates/hourly", methods=["GET"])
    def agg_hourly():
        tz = parse_tz()
        station_ids = parse_station_ids_required()
        start = request.args.get("start")
        end = request.args.get("end")
        agg = request.args.get("agg", "avg")

        def parse_dt(x: Optional[str]) -> Optional[str]:
            if not x: return None
            try:
                dt = datetime.fromisoformat(x.replace("Z","+00:00"))
                local = dt.astimezone(ZoneInfo(DEFAULT_TZ)).replace(tzinfo=None)
                return local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return None

        p_start = parse_dt(start)
        p_end = parse_dt(end)
        time_filter = ""
        params: List[Any] = []
        if p_start:
            time_filter += " AND m.ts >= %s"; params.append(p_start)
        if p_end:
            time_filter += " AND m.ts <= %s"; params.append(p_end)

        sql_template = aggregate_sql("hourly", agg)
        sql = sql_template.format(
            station_ids=",".join(["%s"] * len(station_ids)),
            time_filter=time_filter
        )
        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, tuple(station_ids + params))
            rows = cur.fetchall()

        items = []
        for r in rows:
            bucket = r["bucket"]
            # bucket viene como str/datetime depend. Convertimos:
            if isinstance(bucket, str):
                try:
                    bucket_dt = datetime.strptime(bucket, "%Y-%m-%d %H:%M:%S")
                except:
                    bucket_dt = datetime.strptime(bucket, "%Y-%m-%d")
            else:
                bucket_dt = bucket
            items.append({
                "station_id": r["station_id"],
                "ts": to_iso(bucket_dt, tz),
                "pm25": r["pm2_5"],
                "pm10": r["pm10"],
                "so2":  r["so2"],
                "no2":  r["no2"],
                "o3":   r["o3"],
                "co":   r["co"],
            })
        return jsonify({"granularity": "hourly", "items": items})

    @app.route("/v1/aggregates/daily", methods=["GET"])
    def agg_daily():
        tz = parse_tz()
        station_ids = parse_station_ids_required()
        start = request.args.get("start")
        end = request.args.get("end")
        agg = request.args.get("agg", "avg")

        def parse_dt(x: Optional[str]) -> Optional[str]:
            if not x: return None
            try:
                dt = datetime.fromisoformat(x.replace("Z","+00:00"))
                local = dt.astimezone(ZoneInfo(DEFAULT_TZ)).replace(tzinfo=None)
                return local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return None

        p_start = parse_dt(start)
        p_end = parse_dt(end)
        time_filter = ""
        params: List[Any] = []
        if p_start:
            time_filter += " AND m.ts >= %s"; params.append(p_start)
        if p_end:
            time_filter += " AND m.ts <= %s"; params.append(p_end)

        sql_template = aggregate_sql("daily", agg)
        sql = sql_template.format(
            station_ids=",".join(["%s"] * len(station_ids)),
            time_filter=time_filter
        )
        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, tuple(station_ids + params))
            rows = cur.fetchall()

        items = []
        for r in rows:
            bucket = r["bucket"]
            if isinstance(bucket, str):
                try:
                    bucket_dt = datetime.strptime(bucket, "%Y-%m-%d %H:%M:%S")
                except:
                    bucket_dt = datetime.strptime(bucket, "%Y-%m-%d")
            else:
                bucket_dt = bucket
            items.append({
                "station_id": r["station_id"],
                "ts": to_iso(bucket_dt, tz),
                "pm25": r["pm2_5"],
                "pm10": r["pm10"],
                "so2":  r["so2"],
                "no2":  r["no2"],
                "o3":   r["o3"],
                "co":   r["co"],
            })
        return jsonify({"granularity": "daily", "items": items})

    # ---------- Export CSV ----------

    @app.route("/v1/export/csv", methods=["GET"])
    def export_csv():
        # Opcionalmente protegemos con API key
        # require_api_key()
        tz = parse_tz()
        # Reusamos /v1/measurements multi para construir CSV
        select_clause, _ = build_fields_clause()
        order = (request.args.get("order") or "asc").lower()
        order = "ASC" if order != "desc" else "DESC"

        where = []
        params: List[Any] = []

        station_ids = request.args.getlist("station_id")
        if station_ids:
            where.append("m.station_id IN (" + ",".join(["%s"] * len(station_ids)) + ")")
            params += [int(x) for x in station_ids]

        station_name_csv = request.args.get("station_name")
        if station_name_csv:
            names = [x.strip() for x in station_name_csv.split(",") if x.strip()]
            if names:
                where.append("s.name IN (" + ",".join(["%s"] * len(names)) + ")")
                params += names

        def parse_dt(x: Optional[str]) -> Optional[str]:
            if not x: return None
            try:
                dt = datetime.fromisoformat(x.replace("Z","+00:00"))
                local = dt.astimezone(ZoneInfo(DEFAULT_TZ)).replace(tzinfo=None)
                return local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
        p_start = parse_dt(request.args.get("start"))
        p_end = parse_dt(request.args.get("end"))
        if p_start:
            where.append("m.ts >= %s"); params.append(p_start)
        if p_end:
            where.append("m.ts <= %s"); params.append(p_end)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT s.name AS station_name, {select_clause}
            FROM measurements m
            JOIN stations s ON s.id = m.station_id
            {where_sql}
            ORDER BY m.ts {order}
        """

        with get_conn() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        # construir CSV en memoria
        output = io.StringIO()
        writer = csv.writer(output)
        headers = ["station_name", "ts", "pm25", "pm10", "so2", "no2", "o3", "co"]
        writer.writerow(headers)
        for r in rows:
            md = row_to_measurement_dict(r, tz)
            writer.writerow([
                r["station_name"],
                md["ts"], md["pm25"], md["pm10"], md["so2"], md["no2"], md["o3"], md["co"]
            ])
        output.seek(0)
        return Response(output.getvalue(), mimetype="text/csv; charset=utf-8")

    # ---------- Error handlers ----------
    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"error": "BadRequest", "message": str(e.description)}), 400

    @app.errorhandler(401)
    def unauthorized(e):
        return jsonify({"error": "Unauthorized", "message": str(e.description)}), 401

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "NotFound", "message": str(e.description)}), 404

    @app.errorhandler(500)
    def server_error(e):
        return jsonify({"error": "ServerError", "message": str(e)}), 500

    return app


app = create_app()

if __name__ == "__main__":
    # Modo dev: FLASK_ENV=development si quieres auto-reload
    app.run(host="0.0.0.0", port=8000, debug=True)
