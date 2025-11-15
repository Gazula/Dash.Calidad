from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import os

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Archivo combinado generado desde index.html
COMBINED_FILE = "reporte_combinado.xlsx"


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return RedirectResponse(url="/analizar")


@app.get("/analizar", response_class=HTMLResponse)
async def analizar(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/procesar")
async def procesar(request: Request, archivo: UploadFile = File(...)):
    if archivo.filename.endswith(".xlsx"):
        df = pd.read_excel(archivo.file)
        df.to_excel(COMBINED_FILE, index=False)
    return RedirectResponse(url="/analizar", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not os.path.exists(COMBINED_FILE):
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "error": "No se encontró el archivo procesado."},
        )

    df = pd.read_excel(COMBINED_FILE)

    # Renombrado automático de columnas esperadas
    rename_map = {
        "Fecha de apertura": "fecha_hora_apertura",
        "Fecha apertura": "fecha_hora_apertura",
        "Sub tipo caso": "Sub Tipo Caso",
        "Sub-tipo caso": "Sub Tipo Caso",
        "Definición calidad": "Definición Calidad",
        "Definicion calidad": "Definición Calidad",
        "Código EAN": "EAN",
        "Cod EAN": "EAN",
        "EAN14": "EAN",
        "Lote": "Lote nro.",
        "Proveedor": "Razón social",
        "Razon social": "Razón social",
        "Sucursal": "codigo_sucursal",
        "Código sucursal": "codigo_sucursal"
    }

    df.rename(columns=rename_map, inplace=True)

    # Convertir fecha
    if "fecha_hora_apertura" in df.columns:
        df["fecha_hora_apertura"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce")
        df["Mes"] = df["fecha_hora_apertura"].dt.month
        df["Mes Nombre"] = df["fecha_hora_apertura"].dt.strftime("%B")
    else:
        df["Mes"] = None
        df["Mes Nombre"] = None

    total_reclamos = len(df)

    # Avisos y alertas por tiendas
    if "EAN" in df.columns and "Lote nro." in df.columns and "codigo_sucursal" in df.columns:
        conteo = df.groupby(["EAN", "Lote nro.", "Descripción", "Razón social"]).agg(
            cantidad_tiendas=("codigo_sucursal", "nunique")
        ).reset_index()

        avisos = conteo[(conteo["cantidad_tiendas"] == 2)]
        alertas = conteo[(conteo["cantidad_tiendas"] >= 3)]
    else:
        avisos = pd.DataFrame()
        alertas = pd.DataFrame()

    avisos_list = avisos.to_dict(orient="records")
    alertas_list = alertas.to_dict(orient="records")

    # Selectores dinámicos
    meses = sorted(df["Mes Nombre"].dropna().unique()) if "Mes Nombre" in df else []
    subtipos = sorted(df["Sub Tipo Caso"].dropna().unique()) if "Sub Tipo Caso" in df else []
    definiciones = sorted(df["Definición Calidad"].dropna().unique()) if "Definición Calidad" in df else []
    tiendas = sorted(df["codigo_sucursal"].dropna().unique()) if "codigo_sucursal" in df else []

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "total_reclamos": total_reclamos,
            "avisos": avisos_list,
            "alertas": alertas_list,
            "meses": meses,
            "subtipos": subtipos,
            "definiciones": definiciones,
            "tiendas": tiendas,
        }
    )


@app.get("/reset")
async def reset_filters():
    return RedirectResponse(url="/dashboard", status_code=302)
