from fastapi import FastAPI, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import io
import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BASE_PATH = "bases/Base de datos.xlsx"
RESULT_PATH = "informe_resultado.xlsx"
df_resultado_global = pd.DataFrame()

# ==========================
# Normalizador columnas
# ==========================
def normalizar_col(col):
    return str(col).strip().lower().replace(" ", "").replace(".", "").replace("_", "")

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

    contents = await file.read()
    df_input = pd.read_excel(io.BytesIO(contents))
    df_base = pd.read_excel(BASE_PATH)

    df_input.columns = [c.strip() for c in df_input.columns]
    df_base.columns = [c.strip() for c in df_base.columns]

    merge_col = "EAN" if "EAN" in df_input.columns else df_input.columns[0]
    df_resultado = df_input.merge(df_base, how="left", on=merge_col)

    df_resultado.to_excel(RESULT_PATH, index=False)
    df_resultado_global = df_resultado.copy()

    return templates.TemplateResponse("index.html", {"request": request, "descarga_disponible": True})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, mes: str = "", subtipo: str = "", calidad: str = "",
                    tienda: str = "", busqueda: str = ""):

    global df_resultado_global
    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        df_resultado_global = pd.read_excel(RESULT_PATH)

    df = df_resultado_global.copy()
    if df.empty:
        return HTMLResponse("<h3 style='color:red'>No hay datos cargados todavía.</h3>")

    # Column mapping
    columnas = {
        "fecha": ["fecha de apertura", "fecha/hora de apertura"],
        "ean": ["ean", "codigo ean"],
        "lote": ["lote nro.", "lote", "nro lote"],
        "descripcion": ["descripcion", "producto", "nombre producto"],
        "tienda": ["nombre tienda", "tienda", "sucursal"],
        "razon": ["razon social", "proveedor", "fabricante"]
    }

    mapeo = {}
    for key, pos in columnas.items():
        col = buscar_col(df, pos)
        if col: mapeo[key] = col
        else: df[key] = ""

    df.rename(columns={v:k for k,v in mapeo.items()}, inplace=True)

    # Normalize critical columns
    for col in ["ean", "lote", "tienda"]:
        df[col] = df[col].astype(str).fillna("").str.strip()

    df["mes"] = df["fecha"].astype(str).str[:7]

    df_valid = df[(df["ean"] != "") & (df["tienda"] != "")]
    resumen = df_valid.groupby(["ean", "lote"]).agg(
        cantidad_tiendas=("tienda", lambda x: x.nunique())
    ).reset_index()

    avisos = resumen[resumen["cantidad_tiendas"] == 2]
    alertas = resumen[resumen["cantidad_tiendas"] >= 3]

    info = df_valid[["ean","lote","descripcion","razon"]].drop_duplicates()
    avisos = avisos.merge(info, on=["ean","lote"], how="left")
    alertas = alertas.merge(info, on=["ean","lote"], how="left")

    # === APPLY FILTERS ===
    if mes: avisos = avisos[avisos["ean"].isin(df[df["mes"] == mes]["ean"])]
    if mes: alertas = alertas[alertas["ean"].isin(df[df["mes"] == mes]["ean"])]
    if busqueda:
        avisos = avisos[avisos["descripcion"].str.contains(busqueda, case=False)]
        alertas = alertas[alertas["descripcion"].str.contains(busqueda, case=False)]

    filtros = {
        "meses": sorted(df["mes"].unique()),
        "tiendas": sorted(df["tienda"].unique()),
        "calidad": sorted(df["razon"].unique()),
        "subtipo": sorted(df["descripcion"].unique()),
    }

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df),
        "avisos": avisos.to_dict(orient="records"),
        "alertas": alertas.to_dict(orient="records"),
        "filtros": filtros
    })


@app.get("/exportar/{tipo}")
async def exportar(tipo: str):
    global df_resultado_global
    if tipo.lower() == "avisos":
        df = pd.DataFrame(df_resultado_global)
    else:
        df = pd.DataFrame(df_resultado_global)

    file_name = f"{tipo}_export.xlsx"
    df.to_excel(file_name, index=False)
    return FileResponse(file_name, filename=file_name)
