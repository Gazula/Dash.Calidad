from fastapi import FastAPI, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
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
# FUNCIONES DE NORMALIZACIÓN
# ==========================
def normalizar_col(col):
    col = str(col).strip().lower()
    col = col.replace(" ", "").replace("_", "")
    col = (col.replace("ó", "o")
              .replace("á", "a")
              .replace("é", "e")
              .replace("í", "i")
              .replace("ú", "u"))
    return col


def buscar_col(df, posibles):
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        if normalizar_col(p) in df_cols:
            return df_cols[normalizar_col(p)]
    return None


# ==========================
# RUTA PRINCIPAL
# ==========================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": os.path.exists(RESULT_PATH)
    })


# ==========================
# ANALIZAR ARCHIVO EXCEL
# ==========================
@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    df_input = pd.read_excel(io.BytesIO(contenido))
    df_base = pd.read_excel(BASE_PATH)

    df_input.columns = [c.strip() for c in df_input.columns]
    df_base.columns = [c.strip() for c in df_base.columns]

    merge_key = "EAN" if "EAN" in df_input.columns else df_input.columns[0]
    df_resultado = df_input.merge(df_base, how="left", on=merge_key)

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

    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        df_resultado_global = pd.read_excel(RESULT_PATH)

    if df_resultado_global.empty:
        return HTMLResponse("<h3 style='color:red'>⚠ No hay datos cargados. Vuelva al inicio y cargue un archivo.</h3>")

    df = df_resultado_global.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # ==========================
    # MAPEO DE COLUMNAS
    columnas = {
        "fecha_apertura": ["fecha de apertura", "fecha/hora de apertura"],
        "ean": ["ean", "codigo ean"],
        "lote": ["lote nro.", "lote", "nro lote"],
        "descripcion": ["descripcion", "producto", "nombre producto"],
        "razon_social": ["razon social", "proveedor", "fabricante"],
        "tienda": ["codigo de sucursal", "sucursal", "tienda"],
        "subtipo": ["sub tipo caso", "subtipo", "sub tipo"],
        "definicion": ["definición equipo calidad", "definicion", "calidad", "definicion calidad"]
    }

    mapeo = {}
    for key, posibles in columnas.items():
        col_found = buscar_col(df, posibles)
        if col_found:
            mapeo[key] = col_found
        else:
            df[key] = ""

    df.rename(columns=mapeo, inplace=True)

    # ==========================
    # GENERAR AVISOS Y ALERTAS
    resumen = df.groupby(["ean", "lote"]).size().reset_index(name="cantidad_tiendas")
    avisos = resumen[resumen["cantidad_tiendas"] == 2]
    alertas = resumen[resumen["cantidad_tiendas"] >= 3]

    if not avisos.empty:
        avisos = avisos.merge(df[["ean", "lote", "descripcion", "razon_social", "fecha_apertura"]].drop_duplicates(),
                              on=["ean", "lote"], how="left")

    if not alertas.empty:
        alertas = alertas.merge(df[["ean", "lote", "descripcion", "razon_social", "fecha_apertura"]].drop_duplicates(),
                                on=["ean", "lote"], how="left")

    # ==========================
    # OPCIONES PARA FILTROS
    meses = sorted(df["fecha_apertura"].dropna().astype(str).str[:7].unique())
    tiendas = sorted(df["tienda"].dropna().unique())
    subtipo = sorted(df["subtipo"].dropna().unique())
    definiciones = sorted(df["definicion"].dropna().unique())

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df),
        "avisos": avisos.to_dict(orient="records"),
        "alertas": alertas.to_dict(orient="records"),
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
