# backend/app_liderazgo.py
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
import pandas as pd
from pathlib import Path
from datetime import datetime
import threading, os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

LOCK = threading.Lock()
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
EXCEL_PATH = DATA_DIR / "encuesta_liderazgo.xlsx"
SHEET = "respuestas"
REQUIRED_TOP = {"servicio", "respuestas"}

def _ensure_excel():
  if not EXCEL_PATH.exists():
    cols = ["encuesta_id","servicio","pregunta_id","seccion","pregunta","valor",
            "fortaleza","area_oportunidad","created_at"]
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as xw:
      pd.DataFrame(columns=cols).to_excel(xw, index=False, sheet_name=SHEET)

def _read_df():
  _ensure_excel()
  try:
    return pd.read_excel(EXCEL_PATH, sheet_name=SHEET)
  except Exception:
    return pd.DataFrame(columns=["encuesta_id","servicio","pregunta_id","seccion","pregunta","valor",
                                 "fortaleza","area_oportunidad","created_at"])

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
    # (y las equivalentes con /api/encuesta/…)
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
  rows = [{
    "encuesta_id": encuesta_id,
    "servicio": servicio,
    "pregunta_id": int(r.get("id", 0)),
    "seccion": str(r.get("seccion","")),
    "pregunta": str(r.get("pregunta","")),
    "valor": int(r.get("valor", 0)),
    "fortaleza": fortaleza,
    "area_oportunidad": area_op,
    "created_at": created_at
  } for r in respuestas]
  df_new = pd.DataFrame(rows)

  with LOCK:
    _ensure_excel()
    try:
      df_old = pd.read_excel(EXCEL_PATH, sheet_name=SHEET)
    except Exception:
      df_old = pd.DataFrame(columns=df_new.columns)
    df_out = pd.concat([df_old, df_new], ignore_index=True)
    tmp = EXCEL_PATH.with_suffix(".tmp.xlsx")
    with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
      df_out.to_excel(xw, index=False, sheet_name=SHEET)
    tmp.replace(EXCEL_PATH)

  return jsonify(ok=True, encuesta_id=encuesta_id, guardadas=len(rows), path=request.path)

# ── Listar servicios
@dual_route("/encuesta/servicios", methods=["GET"])
def listar_servicios():
  df = _read_df()
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
  df = _read_df()
  if df.empty:
    return jsonify(ok=True, servicio=servicio, encuestas=0, preguntas=[], comentarios=[])
  df_s = df[df["servicio"].astype(str) == servicio]
  if df_s.empty:
    return jsonify(ok=True, servicio=servicio, encuestas=0, preguntas=[], comentarios=[])

  total_encuestas = df_s["encuesta_id"].nunique()

  prom = (df_s.groupby(["pregunta_id","seccion","pregunta"])
               .agg(promedio=("valor","mean"), respuestas=("valor","size"))
               .reset_index().sort_values("pregunta_id"))

  dist = (df_s.groupby(["pregunta_id","valor"]).size()
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

  comentarios = (df_s[["encuesta_id","fortaleza","area_oportunidad","created_at"]]
                  .drop_duplicates()
                  .sort_values("created_at", ascending=False)
                  .head(100)
                  .to_dict(orient="records"))

  return jsonify(ok=True,
                 servicio=servicio,
                 encuestas=total_encuestas,
                 preguntas=preguntas,
                 comentarios=comentarios)

# ── Exportar Excel
@dual_route("/encuesta/export", methods=["GET"])
def export_excel():
  _ensure_excel()
  return send_file(EXCEL_PATH, as_attachment=True, download_name="encuesta_liderazgo.xlsx")

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=6010, debug=True, use_reloader=False)
