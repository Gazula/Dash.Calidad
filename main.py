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

BASE_PATH = "bases/Base de datos.xlsx"
RESULT_PATH = "informe_resultado.xlsx"

df_resultado_global = pd.DataFrame()

# Normalización de texto
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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "descarga_disponible": False})


@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    df_input = pd.read_excel(io.BytesIO(contenido))
    df_base = pd.read_excel(BASE_PATH)

    df_input.columns = [c.strip() for c in df_input.columns]
    df_base.columns = [c.strip() for c in df_base.columns]

    col_ean_input = buscar_col(df_input, ["ean", "codigo ean", "ean13", "ean 13"])
    col_ean_base = buscar_col(df_base, ["ean", "codigo ean", "ean13", "ean 13"])

    if not col_ean_input or not col_ean_base:
        return HTMLResponse("<h3 style='color:red'>⚠ No se encontró la columna EAN en el archivo cargado.</h3>")

    df_resultado = df_input.merge(df_base, on=col_ean_input, how="left", suffixes=("", "_base"))
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
        return HTMLResponse("<h3 style='color:red'>⚠ No hay datos disponibles. Primero cargá un archivo.</h3>")

    df = df_resultado_global.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Posibles columnas esperadas
    columnas_posibles = {
        "fecha_apertura": ["fecha/hora de apertura", "fecha de apertura", "fecha"],
        "ean": ["ean", "codigo ean", "ean13", "ean 13"],
        "lote": ["lote nro.", "lote", "nro lote"],
        "descripcion": ["descripcion", "producto", "nombre producto"],
        "razon_social": ["razon social", "proveedor", "fabricante"],
        "tienda": ["codigo de sucursal", "sucursal", "tienda", "local"]
    }

    rename_map = {}
    for key, posibles in columnas_posibles.items():
        found = buscar_col(df, posibles)
        if found:
            rename_map[found] = key
        else:
            df[key] = pd.NA  # columna vacía si no existe

    df.rename(columns=rename_map, inplace=True)

    # Eliminar duplicados de columna
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Convertir columnas a string para evitar errores
    for col in ["ean", "lote", "tienda"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("")

    # Avisos y Alertas
    avisos = pd.DataFrame()
    alertas = pd.DataFrame()

    if {"ean", "lote", "tienda"}.issubset(df.columns):
        resumen = df.groupby(["ean", "lote"]).agg(
            cantidad_tiendas=("tienda", lambda x: x.nunique(dropna=True))
        ).reset_index()

        avisos = resumen[resumen["cantidad_tiendas"] == 2]
        alertas = resumen[resumen["cantidad_tiendas"] >= 3]

        info_cols = df[["ean", "lote", "descripcion", "razon_social"]].drop_duplicates()
        avisos = avisos.merge(info_cols, on=["ean", "lote"], how="left")
        alertas = alertas.merge(info_cols, on=["ean", "lote"], how="left")

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df),
        "avisos": avisos.to_dict(orient="records") if not avisos.empty else [],
        "alertas": alertas.to_dict(orient="records") if not alertas.empty else [],
    })


@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx")
    return {"error": "⚠ No hay informe disponible."}
