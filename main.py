from fastapi import FastAPI, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os
import io

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BASE_PATH = "bases/Base de datos.xlsx"
RESULT_PATH = "informe_resultado.xlsx"
df_resultado_global = pd.DataFrame()


# ==========================
# UTIL - NORMALIZACIÓN NOMBRES
# ==========================
def normalizar_col(col):
    col = str(col or "").strip().lower()
    col = col.replace(" ", "").replace("_", "")
    col = (col.replace("ó", "o")
              .replace("á", "a")
              .replace("é", "e")
              .replace("í", "i")
              .replace("ú", "u"))
    return col


def buscar_col(df, posibles):
    """Devuelve el nombre real de la columna en df que coincide con alguna 'posible'."""
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        key = normalizar_col(p)
        if key in df_cols:
            return df_cols[key]
    return None


# ==========================
# RUTA INDEX / UPLOAD
# ==========================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": os.path.exists(RESULT_PATH)
    })


@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    try:
        df_input = pd.read_excel(io.BytesIO(contenido))
    except Exception as e:
        return templates.TemplateResponse("index.html", {"request": request, "error": f"Error leyendo Excel: {e}"})

    # Intento cargar base (si existe)
    if os.path.exists(BASE_PATH):
        df_base = pd.read_excel(BASE_PATH)
    else:
        df_base = pd.DataFrame()

    # limpiar nombres
    df_input.columns = [str(c).strip() for c in df_input.columns]
    df_base.columns = [str(c).strip() for c in df_base.columns]

    # buscar columna EAN en input y base (de forma flexible)
    posibles_ean = ["ean", "codigo ean", "cod ean", "ean13", "ean 13", "codigo"]
    col_ean_input = buscar_col(df_input, posibles_ean)
    col_ean_base = buscar_col(df_base, posibles_ean) if not df_base.empty else None

    if col_ean_input and col_ean_base:
        df_resultado = df_input.merge(df_base, how="left", left_on=col_ean_input, right_on=col_ean_base)
    elif col_ean_input and df_base.empty:
        # no hay base, seguimos solo con input
        df_resultado = df_input.copy()
    elif col_ean_input:
        # base existe pero no se encontró ean en base; hacemos left merge por col_ean_input vs cualquier 'EAN' en base
        df_resultado = df_input.copy()
    else:
        return templates.TemplateResponse("index.html", {"request": request, "error": "No se encontró columna EAN en el archivo cargado."})

    # Guardar resultado para uso posterior
    df_resultado.to_excel(RESULT_PATH, index=False)
    df_resultado_global = df_resultado.copy()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": True
    })


# ==========================
# DASHBOARD
# ==========================
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    global df_resultado_global

    # si no está en memoria, intento leer el archivo guardado
    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        try:
            df_resultado_global = pd.read_excel(RESULT_PATH)
        except Exception as e:
            return HTMLResponse(f"<h3 style='color:red'>Error leyendo informe guardado: {e}</h3>")

    if df_resultado_global.empty:
        return HTMLResponse("<h3 style='color:red'>⚠ No hay datos cargados. Subí un archivo primero en la página principal.</h3>")

    df = df_resultado_global.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # ======================================================
    # DEFINIMOS LISTA DE POSIBLES NOMBRES PARA CADA CAMPO
    # ======================================================
    columnas_posibles = {
        "fecha_apertura": ["fecha/hora de apertura", "fecha de apertura", "fecha apertura", "fecha"],
        "ean": ["ean", "codigo ean", "cod ean", "ean13", "ean 13"],
        "lote": ["lote nro.", "lote", "nro lote"],
        "descripcion": ["descripcion", "descripcion producto", "producto", "nombre producto"],
        "razon_social": ["razon social", "proveedor", "fabricante", "razon_social"],
        "tienda": ["codigo de sucursal", "sucursal", "tienda", "codigo_sucursal"],
        "subtipo": ["sub tipo caso", "subtipo", "sub_tipo_caso"],
        "definicion": ["definicion equipo calidad", "definicion calidad", "definicion", "equipocalidad"]
    }

    # ======================================================
    # BUSCAMOS Y RENOMBRAMOS A NOMBRES ESTANDAR: 'ean','lote', etc.
    # ======================================================
    rename_map = {}
    for key, posibles in columnas_posibles.items():
        found = buscar_col(df, posibles)
        if found:
            rename_map[found] = key  # renombramos la columna real al nombre estándar
        else:
            # creamos columna vacía para evitar KeyError más adelante
            df[key] = pd.NA

    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # ahora deberíamos tener columnas estándar: 'ean', 'lote', 'descripcion', 'razon_social', 'fecha_apertura', 'tienda', 'subtipo', 'definicion'
    # convertimos fecha si está presente
    if "fecha_apertura" in df.columns:
        try:
            df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"], errors="coerce")
        except Exception:
            df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"].astype(str), errors="coerce")

    # ======================================================
    # AGRUPAR PARA AVISOS / ALERTAS (solo si columnas existen)
    # ======================================================
    avisos = pd.DataFrame()
    alertas = pd.DataFrame()
    if "ean" in df.columns and "lote" in df.columns:
        # rellenamos nulos por si hay NaN al agrupar
        df_agg = df.copy()
        df_agg["ean"] = df_agg["ean"].astype(str).fillna("")
        df_agg["lote"] = df_agg["lote"].astype(str).fillna("")
        resumen = df_agg.groupby(["ean", "lote"], dropna=False).agg(cantidad_tiendas=("tienda", lambda s: s.nunique(dropna=True))).reset_index()
        avisos = resumen[resumen["cantidad_tiendas"] == 2].copy()
        alertas = resumen[resumen["cantidad_tiendas"] >= 3].copy()

        # agregar descripción y proveedor si están disponibles en df
        join_cols = [c for c in ["ean", "lote", "descripcion", "razon_social", "fecha_apertura"] if c in df.columns]
        if join_cols:
            df_unique = df[join_cols].drop_duplicates(subset=["ean", "lote"]) if {"ean", "lote"}.issubset(join_cols) else pd.DataFrame()
            if not df_unique.empty:
                if not avisos.empty:
                    avisos = avisos.merge(df_unique, on=["ean", "lote"], how="left")
                if not alertas.empty:
                    alertas = alertas.merge(df_unique, on=["ean", "lote"], how="left")
    else:
        # si faltan columnas clave, dejamos avisos/alertas vacíos y no rompemos
        avisos = pd.DataFrame()
        alertas = pd.DataFrame()

    # ======================================================
    # OPCIONES PARA FILTROS (generar listas seguras)
    # ======================================================
    # Meses en formato YYYY-MM si fecha_apertura existe
    meses = []
    if "fecha_apertura" in df.columns and not df["fecha_apertura"].isna().all():
        meses = sorted(df["fecha_apertura"].dropna().dt.to_period("M").astype(str).unique().tolist())

    tiendas = sorted(df["tienda"].dropna().astype(str).unique().tolist()) if "tienda" in df.columns else []
    subtipo = sorted(df["subtipo"].dropna().astype(str).unique().tolist()) if "subtipo" in df.columns else []
    definiciones = sorted(df["definicion"].dropna().astype(str).unique().tolist()) if "definicion" in df.columns else []

    # pasar a dicts para template (si tablas vacías se envía lista vacía)
    avisos_list = avisos.to_dict(orient="records") if not avisos.empty else []
    alertas_list = alertas.to_dict(orient="records") if not alertas.empty else []

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df),
        "avisos": avisos_list,
        "alertas": alertas_list,
        "meses": meses,
        "tiendas": tiendas,
        "subtipo": subtipo,
        "definiciones": definiciones
    })


# ==========================
# DESCARGA EXCEL
# ==========================
@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx")
    return {"error": "No hay informe disponible."}
