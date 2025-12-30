# backend/app_liderazgo.py
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
import pandas as pd
from pathlib import Path
from datetime import datetime
import threading, os
import pyodbc # Nuevo: Conector SQL Server

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

LOCK = threading.Lock()

# ===================== CONFIGURACIÓN SQL SERVER =====================
SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-EO74OCH\\SQLEXPRESS;"
    "Database=punta_medica;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

def get_db_connection():
    return pyodbc.connect(SQL_CONN_STR)

# ===================== INICIALIZACIÓN DE TABLA SQL =====================
def init_db_liderazgo():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EncuestaLiderazgo' AND xtype='U')
        CREATE TABLE EncuestaLiderazgo (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            encuesta_id VARCHAR(50),
            servicio VARCHAR(255),
            pregunta_id INT,
            seccion VARCHAR(255),
            pregunta VARCHAR(MAX),
            valor INT,
            fortaleza VARCHAR(MAX),
            area_oportunidad VARCHAR(MAX),
            created_at VARCHAR(100)
        )
    ''')
    conn.commit()
    conn.close()

init_db_liderazgo()

# --- ORIGINAL (EXCEL) ---
# DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
# EXCEL_PATH = DATA_DIR / "encuesta_liderazgo.xlsx"
# SHEET = "respuestas"
REQUIRED_TOP = {"servicio", "respuestas"}

# --- HELPERS ORIGINALES (COMENTADOS) ---
# def _ensure_excel(): ...
# def _read_df(): ...

# ── errores JSON
@app.errorhandler(404)
def not_found(e): return jsonify(ok=False, error="Ruta no encontrada", path=request.path), 404
@app.errorhandler(405)
def not_allowed(e): return jsonify(ok=False, error="Método no permitido", path=request.path), 405
@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return jsonify(ok=False, error=e.description, code=e.code), e.code
    return jsonify(ok=False, error=str(e)), 500

# ── decorador para registrar con y sin /api
def dual_route(rule, **options):
    def decorator(f):
        app.add_url_rule(rule, endpoint=f.__name__+rule, view_func=f, **options)
        api_rule = "/api" + (rule if rule.startswith("/") else "/" + rule)
        app.add_url_rule(api_rule, endpoint=f.__name__+api_rule, view_func=f, **options)
        return f
    return decorator

# ── Health / Help / Routes
@dual_route("/encuesta/ping", methods=["GET"])
def ping(): return jsonify(ok=True, ts=datetime.utcnow().isoformat(), path=request.path)

@dual_route("/encuesta/help", methods=["GET"])
def help_():
    return jsonify(ok=True, endpoints=[
        "GET  /encuesta/ping",
        "POST /encuesta",
        "GET  /encuesta/servicios",
        "GET  /encuesta/promedio/<servicio>",
        "GET  /encuesta/export",
        "GET  /encuesta/routes",
    ])

@dual_route("/encuesta/routes", methods=["GET"])
def routes():
    rules = sorted([str(r) for r in app.url_map.iter_rules()])
    return jsonify(ok=True, routes=rules)

# ── Guardar encuesta
@dual_route("/encuesta", methods=["POST"])
def guardar_encuesta():
    payload = request.get_json(silent=True) or {}
    if not REQUIRED_TOP.issubset(payload.keys()):
        return jsonify(ok=False, error="Payload incompleto"), 400

    servicio = str(payload.get("servicio","")).strip()
    if not servicio: return jsonify(ok=False, error="Servicio vacío"), 400

    created_at = str(payload.get("created_at","")).strip() or datetime.utcnow().isoformat()
    comentarios = payload.get("comentarios") or {}
    fortaleza = str(comentarios.get("fortaleza","")).strip()
    area_op = str(comentarios.get("area_oportunidad","")).strip()

    respuestas = payload.get("respuestas") or []
    if not isinstance(respuestas, list) or not respuestas:
        return jsonify(ok=False, error="Respuestas vacías"), 400

    encuesta_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    
    # --- NUEVA LÓGICA SQL SERVER ---
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for r in respuestas:
            cursor.execute("""
                INSERT INTO EncuestaLiderazgo 
                (encuesta_id, servicio, pregunta_id, seccion, pregunta, valor, fortaleza, area_oportunidad, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                encuesta_id, servicio, int(r.get("id", 0)), str(r.get("seccion","")),
                str(r.get("pregunta","")), int(r.get("valor", 0)), fortaleza, area_op, created_at
            ))
        
        conn.commit()
        conn.close()

        # --- ORIGINAL (EXCEL COMENTADO) ---
        # rows = [...]
        # df_new = pd.DataFrame(rows)
        # with LOCK: ... (lógica de guardado en Excel)

        return jsonify(ok=True, encuesta_id=encuesta_id, guardadas=len(respuestas), path=request.path)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

# ── Listar servicios
@dual_route("/encuesta/servicios", methods=["GET"])
def listar_servicios():
    # --- NUEVA LÓGICA SQL SERVER ---
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM EncuestaLiderazgo", conn)
    conn.close()

    # --- ORIGINAL (EXCEL COMENTADO) ---
    # df = _read_df()

    if df.empty: return jsonify(ok=True, servicios=[])
    
    grp = (df.groupby("servicio")
               .agg(encuestas=("encuesta_id","nunique"),
                    registros=("encuesta_id","size"),
                    ultima_fecha=("created_at","max"))
               .reset_index()
               .sort_values(["encuestas","servicio"], ascending=[False, True]))
    return jsonify(ok=True, servicios=grp.to_dict(orient="records"))

# ── Promedios por servicio
@dual_route("/encuesta/promedio/<path:servicio>", methods=["GET"])
def promedio_por_servicio(servicio):
    # --- NUEVA LÓGICA SQL SERVER ---
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM EncuestaLiderazgo WHERE servicio = ?", conn, params=[servicio])
    conn.close()

    # --- ORIGINAL (EXCEL COMENTADO) ---
    # df = _read_df()
    # df_s = df[df["servicio"].astype(str) == servicio]

    if df.empty:
        return jsonify(ok=True, servicio=servicio, encuestas=0, preguntas=[], comentarios=[])

    total_encuestas = df["encuesta_id"].nunique()

    prom = (df.groupby(["pregunta_id","seccion","pregunta"])
                 .agg(promedio=("valor","mean"), respuestas=("valor","size"))
                 .reset_index().sort_values("pregunta_id"))

    dist = (df.groupby(["pregunta_id","valor"]).size()
              .reset_index(name="conteo")
              .pivot_table(index="pregunta_id", columns="valor",
                           values="conteo", fill_value=0)
              .reindex(columns=[1,2,3,4,5], fill_value=0))

    prom = prom.merge(dist, on="pregunta_id", how="left")
    prom["promedio"] = prom["promedio"].round(2)

    preguntas = []
    for _, row in prom.iterrows():
        preguntas.append({
          "pregunta_id": int(row["pregunta_id"]),
          "seccion": row["seccion"],
          "pregunta": row["pregunta"],
          "promedio": float(row["promedio"]),
          "respuestas": int(row["respuestas"]),
          "dist": {str(k): int(row.get(k, 0)) for k in [1,2,3,4,5]}
        })

    comentarios = (df[["encuesta_id","fortaleza","area_oportunidad","created_at"]]
                      .drop_duplicates()
                      .sort_values("created_at", ascending=False)
                      .head(100)
                      .to_dict(orient="records"))

    return jsonify(ok=True,
                   servicio=servicio,
                   encuestas=total_encuestas,
                   preguntas=preguntas,
                   comentarios=comentarios)

# ── Exportar Excel (Generado desde SQL)
@dual_route("/encuesta/export", methods=["GET"])
def export_excel():
    # --- NUEVA LÓGICA SQL SERVER ---
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM EncuestaLiderazgo", conn)
    conn.close()
    
    export_path = "export_liderazgo.xlsx"
    df.to_excel(export_path, index=False)
    return send_file(export_path, as_attachment=True, download_name="encuesta_liderazgo.xlsx")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6010, debug=True, use_reloader=False)