from fastapi import FastAPI, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import os
import io

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

RESULT_PATH = "informe_resultado.xlsx"
df_resultado_global = pd.DataFrame()

# ======================================================
# Normalización de nombres de columnas
# ======================================================
def normalizar_col(col):
    c = str(col).strip().lower()
    c = c.replace(" ", "").replace("_", "")
    c = (c.replace("á","a").replace("é","e")
           .replace("í","i").replace("ó","o")
           .replace("ú","u"))
    return c

def buscar_col(df, posibles):
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        key = normalizar_col(p)
        if key in df_cols:
            return df_cols[key]
    return None


# ======================================================
# Página principal
# ======================================================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ======================================================
# Procesar archivo
# ======================================================
@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    df = pd.read_excel(io.BytesIO(contenido))

    df.columns = [str(c).strip() for c in df.columns]
    df_resultado_global = df.copy()

    df.to_excel(RESULT_PATH, index=False)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": True
    })


# ======================================================
# Dashboard
# ======================================================
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    global df_resultado_global

    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        df_resultado_global = pd.read_excel(RESULT_PATH)

    if df_resultado_global.empty:
        return HTMLResponse("<h3>No hay datos cargados.</h3>")

    df = df_resultado_global.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Mapeo de columnas
    columnas = {
        "fecha": ["fecha de apertura","fecha","fecha_apertura","fecha/hora de apertura"],
        "ean": ["ean","codigo ean","cod ean"],
        "lote": ["lote","lote nro.","nro lote"],
        "descripcion": ["descripcion","producto","nombre producto"],
        "proveedor": ["razon social","proveedor","fabricante"],
        "tienda": ["tienda","sucursal","codigo_sucursal"],
        "mes": ["mes"],
        "subtipo": ["sub tipo caso","subtipo","subtipo caso"],
        "calidad": ["definicion calidad","def calidad","calidad"]
    }

    # Normalización y renombre
    for key, posibles in columnas.items():
        col = buscar_col(df, posibles)
        if col:
            df.rename(columns={col: key}, inplace=True)
        else:
            df[key] = ""

    # =============================================================
    # NORMALIZACIÓN QUE FALTABA 🔥
    # =============================================================

    # Normalizar EAN
    df["ean"] = df["ean"].astype(str).str.replace(".0", "", regex=False).str.strip()

    # Normalizar Lote
    df["lote"] = df["lote"].astype(str).str.strip().replace(
        ["nan","None","none","NaN"], ""
    )

    # FILTRAR reclamos SIN LOTE
    df = df[df["lote"] != ""]

    # =============================================================
    # Avisos y alertas
    # =============================================================
    resumen = df.groupby(["ean", "lote"]).size().reset_index(name="cantidad_tiendas")

    avisos = resumen[resumen["cantidad_tiendas"] == 2]
    alertas = resumen[resumen["cantidad_tiendas"] >= 3]

    info = df[["ean","lote","descripcion","proveedor"]].drop_duplicates()

    avisos = avisos.merge(info, on=["ean","lote"], how="left")
    alertas = alertas.merge(info, on=["ean","lote"], how="left")

    filtros = {
        "meses": sorted(df["mes"].unique()),
        "subtipos": sorted(df["subtipo"].unique()),
        "calidades": sorted(df["calidad"].unique()),
        "tiendas": sorted(df["tienda"].unique())
    }

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df),
        "avisos": avisos.to_dict(orient="records"),
        "alertas": alertas.to_dict(orient="records"),
        "filtros": filtros
    })


# ======================================================
# Análisis avanzado
# ======================================================
@app.get("/analisis", response_class=HTMLResponse)
async def analisis(request: Request):
    global df_resultado_global

    if df_resultado_global.empty:
        return HTMLResponse("<h3>No hay datos cargados.</h3>")

    df = df_resultado_global.copy()
    df.columns = [str(c).strip() for c in df.columns]

    return templates.TemplateResponse("analisis.html", {
        "request": request,
    })


# ======================================================
@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx")
    return {"error": "No existe archivo"}
