from fastapi import FastAPI, UploadFile, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os
import io

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

RESULT_PATH = "informe_resultado.xlsx"
df_resultado_global = pd.DataFrame()

def normalizar_col(col):
    col = str(col).strip().lower()
    col = col.replace(" ", "").replace("_","")
    col = (col.replace("ó","o").replace("á","a").replace("é","e")
              .replace("í","i").replace("ú","u"))
    return col

def buscar_col(df, posibles):
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        if normalizar_col(p) in df_cols:
            return df_cols[normalizar_col(p)]
    return None

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "descarga_disponible": False})

@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    df_input = pd.read_excel(io.BytesIO(contenido))

    df_input.columns = [c.strip() for c in df_input.columns]
    df_resultado = df_input.copy()

    df_resultado.to_excel(RESULT_PATH, index=False)
    df_resultado_global = df_resultado.copy()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": True
    })

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    global df_resultado_global

    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        df_resultado_global = pd.read_excel(RESULT_PATH)

    if df_resultado_global.empty:
        return HTMLResponse("<h3 style='color:red'>No hay datos. Cargá un archivo primero.</h3>")

    df = df_resultado_global.copy()
    df.columns = [str(c).strip() for c in df.columns]

    columnas = {
        "fecha": ["fecha de apertura","fecha/hora de apertura","fecha"],
        "ean": ["ean","codigo ean","cod ean"],
        "lote": ["lote nro.","lote","nro lote"],
        "descripcion": ["descripcion","producto","nombre producto"],
        "razon": ["razon social","proveedor","fabricante"],
        "tienda": ["codigo_sucursal","sucursal","tienda"],
        "mes": ["mes"],
        "subtipo": ["sub tipo caso","subtipo"],
        "calidad": ["definicion_calidad","calidad","def calidad"]
    }

    mapeo = {}
    for key, posibles in columnas.items():
        col = buscar_col(df, posibles)
        if col: mapeo[key] = col

    for key in columnas:
        if key not in mapeo:
            df[key] = ""
        else:
            df.rename(columns={mapeo[key]: key}, inplace=True)

    # Convertir todo a texto para evitar errores
    for col in ["ean","lote","descripcion","razon","tienda","mes","subtipo","calidad"]:
        df[col] = df[col].fillna("").astype(str)

    resumen = df.groupby(["ean","lote"]).size().reset_index(name="cantidad_tiendas")

    avisos = resumen[resumen["cantidad_tiendas"] == 2]
    alertas = resumen[resumen["cantidad_tiendas"] >= 3]

    if not avisos.empty:
        avisos = avisos.merge(df[["ean","lote","descripcion","razon"]].drop_duplicates(), on=["ean","lote"], how="left")
    if not alertas.empty:
        alertas = alertas.merge(df[["ean","lote","descripcion","razon"]].drop_duplicates(), on=["ean","lote"], how="left")

    filtros = {
        "meses": sorted(df["mes"].unique()),
        "subtipo": sorted(df["subtipo"].unique()),
        "calidad": sorted(df["calidad"].unique()),
        "tiendas": sorted(df["tienda"].unique())
    }

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df),
        "avisos": avisos.to_dict(orient="records"),
        "alertas": alertas.to_dict(orient="records"),
        "filtros": filtros
    })

@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx")
    return {"error":"No disponible"}
