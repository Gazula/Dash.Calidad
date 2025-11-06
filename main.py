from fastapi import FastAPI, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import List
import pandas as pd
import io
import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BASE_PATH = "bases/Base de datos.xlsx"
RESULT_PATH = "informe_resultado.xlsx"


# === INDEX ===
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "descarga_disponible": False})


@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    contenido = await file.read()
    df_input = pd.read_excel(io.BytesIO(contenido))
    df_base = pd.read_excel(BASE_PATH)

    df_input.columns = [c.strip().lower() for c in df_input.columns]
    df_base.columns = [c.strip().lower() for c in df_base.columns]

    if "ean" not in df_input.columns:
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": "El archivo no contiene la columna 'EAN'",
            "descarga_disponible": False
        })

    df_resultado = df_input.merge(df_base, on="ean", how="left", suffixes=("", "_base"))

    if "fecha/hora de apertura" in df_resultado.columns:
        df_resultado["fecha/hora de apertura"] = pd.to_datetime(df_resultado["fecha/hora de apertura"], errors="coerce")
        df_resultado["fecha de apertura"] = df_resultado["fecha/hora de apertura"].dt.date
        df_resultado["hora de apertura"] = df_resultado["fecha/hora de apertura"].dt.time

    df_resultado.rename(columns={
        "descripcion": "Descripción",
        "razon_social": "Razón social",
        "definicion_calidad": "Definición equipo calidad",
        "sub_tipo_caso": "Sub Tipo Caso"
    }, inplace=True)

    df_resultado.to_excel(RESULT_PATH, index=False)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "mensaje": "Archivo procesado correctamente. Podés descargarlo o ver el dashboard.",
        "descarga_disponible": True
    })


@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx",
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return {"error": "No hay informe disponible."}


# === DASHBOARD ===
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not os.path.exists(RESULT_PATH):
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": "No se encontró informe para mostrar el dashboard."
        })

    df = pd.read_excel(RESULT_PATH)

    # Columnas esperadas
    for col in ["fecha de apertura", "Sub Tipo Caso", "Definición equipo calidad", "Descripción", "Razón social"]:
        if col not in df.columns:
            df[col] = None

    # Filtros únicos
    meses = sorted(df["fecha de apertura"].dropna().astype(str).unique().tolist())
    subtipos = sorted(df["Sub Tipo Caso"].dropna().unique().tolist())
    definiciones = sorted(df["Definición equipo calidad"].dropna().unique().tolist())

    # Avisos y alertas
    avisos = df[df["Definición equipo calidad"].str.contains("aviso", case=False, na=False)]
    alertas = df[df["Definición equipo calidad"].str.contains("alerta", case=False, na=False)]

    # Convertimos los DataFrames a listas de dicts para listados
    avisos_list = avisos[["fecha de apertura", "EAN", "Lote nro.", "Descripción", "Razón social"]].fillna("").to_dict(orient="records")
    alertas_list = alertas[["fecha de apertura", "EAN", "Lote nro.", "Descripción", "Razón social"]].fillna("").to_dict(orient="records")

    total_reclamos = len(df)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": total_reclamos,
        "avisos": avisos_list,
        "alertas": alertas_list,
        "meses": meses,
        "subtipos": subtipos,
        "definiciones": definiciones
    })
