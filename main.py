from fastapi import FastAPI, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import List
import pandas as pd
import io
import plotly.express as px
import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BASE_PATH = "bases/Base de datos.xlsx"
RESULT_PATH = "informe_resultado.xlsx"
df_resultado_global = pd.DataFrame()

# Columnas finales (con mayúsculas exactas)
COLUMNAS_INFORME = [
    "Número del caso",
    "Fecha de apertura",
    "Hora de apertura",
    "Código de sucursal",
    "Respuesta tienda",
    "Definición equipo calidad",
    "Estado",
    "EAN",
    "Categoría",
    "Lote nro.",
    "Fecha de vencimiento",
    "Descripción",
    "Razón social",
]

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "tabla": None,
        "grafico_html": None,
        "grafico_secundario": None,
        "descarga_disponible": False,
        "proveedores": [],
        "eans": [],
        "productos": [],
        "definiciones": [],
        "columnas": [],
        "filtros": {"proveedor": [], "ean": [], "descripcion": [], "definicion": []},
        "tipo_grafico": "barras",
        "variable_x": "",
        "variable_y": "count",
    })


@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    df_input = pd.read_excel(io.BytesIO(contenido))
    df_base = pd.read_excel(BASE_PATH)

    # Normalizamos los nombres de columnas
    df_input.columns = [c.strip().lower() for c in df_input.columns]
    df_base.columns = [c.strip().lower() for c in df_base.columns]

    if "ean" not in df_input.columns:
        return templates.TemplateResponse("index.html", {
            "request": request,
            "tabla": "<p style='color:red'>El archivo no contiene la columna 'EAN'</p>",
            "grafico_html": None, "grafico_secundario": None, "descarga_disponible": False,
            "proveedores": [], "eans": [], "productos": [], "definiciones": [], "columnas": [],
            "filtros": {"proveedor": [], "ean": [], "descripcion": [], "definicion": []},
            "tipo_grafico": "barras", "variable_x": "", "variable_y": "count"
        })

    # Merge con base
    df_resultado = df_input.merge(df_base, on="ean", how="left", suffixes=("", "_base"))

    # Dividir fecha y hora
    if "fecha/hora de apertura" in df_resultado.columns:
        df_resultado["fecha/hora de apertura"] = pd.to_datetime(df_resultado["fecha/hora de apertura"], errors="coerce")
        df_resultado["fecha de apertura"] = df_resultado["fecha/hora de apertura"].dt.date
        df_resultado["hora de apertura"] = df_resultado["fecha/hora de apertura"].dt.time
    else:
        df_resultado["fecha de apertura"] = None
        df_resultado["hora de apertura"] = None

    df_resultado["Tipificación"] = df_resultado.get("descripcion", "No tipificado").fillna("No tipificado")

    # Reajustar nombres (con mayúsculas exactas)
    df_resultado_ren = {}
    for c in df_resultado.columns:
        c_limpia = c.strip().lower()
        if c_limpia == "número del caso":
            df_resultado_ren[c] = "Número del caso"
        elif c_limpia == "fecha de apertura":
            df_resultado_ren[c] = "Fecha de apertura"
        elif c_limpia == "hora de apertura":
            df_resultado_ren[c] = "Hora de apertura"
        elif c_limpia == "codigo de sucursal":
            df_resultado_ren[c] = "Código de sucursal"
        elif c_limpia == "respuesta tienda":
            df_resultado_ren[c] = "Respuesta tienda"
        elif c_limpia == "definición equipo calidad":
            df_resultado_ren[c] = "Definición equipo calidad"
        elif c_limpia == "estado":
            df_resultado_ren[c] = "Estado"
        elif c_limpia == "ean":
            df_resultado_ren[c] = "EAN"
        elif c_limpia == "categoría":
            df_resultado_ren[c] = "Categoría"
        elif c_limpia == "lote nro.":
            df_resultado_ren[c] = "Lote nro."
        elif c_limpia == "fecha de vencimiento":
            df_resultado_ren[c] = "Fecha de vencimiento"
        elif c_limpia == "descripcion":
            df_resultado_ren[c] = "Descripción"
        elif c_limpia == "razon_social":
            df_resultado_ren[c] = "Razón social"

    df_resultado.rename(columns=df_resultado_ren, inplace=True)

    # Filtrar solo las columnas de interés
    columnas_finales = [c for c in COLUMNAS_INFORME if c in df_resultado.columns]
    df_resultado = df_resultado[columnas_finales]

    df_resultado.to_excel(RESULT_PATH, index=False)
    df_resultado_global = df_resultado.copy()

    proveedores = sorted(df_resultado["Razón social"].dropna().unique().tolist())
    eans = sorted(df_resultado["EAN"].dropna().unique().astype(str).tolist())
    productos = sorted(df_resultado["Descripción"].dropna().unique().tolist())
    definiciones = sorted(df_resultado["Definición equipo calidad"].dropna().unique().tolist()) if "Definición equipo calidad" in df_resultado.columns else []
    columnas = sorted(df_resultado.columns)

    tabla_html = df_resultado.head(50).to_html(classes="table table-striped", index=False)
    grafico_html = crear_grafico(df_resultado, "barras", "Razón social", "count")
    grafico_secundario = crear_grafico_top_productos(df_resultado)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "tabla": tabla_html,
        "grafico_html": grafico_html,
        "grafico_secundario": grafico_secundario,
        "descarga_disponible": True,
        "proveedores": proveedores,
        "eans": eans,
        "productos": productos,
        "definiciones": definiciones,
        "columnas": columnas,
        "filtros": {"proveedor": [], "ean": [], "descripcion": [], "definicion": []},
        "tipo_grafico": "barras",
        "variable_x": "Razón social",
        "variable_y": "count"
    })


@app.post("/filtrar", response_class=HTMLResponse)
async def filtrar(
    request: Request,
    proveedor: List[str] = Form([]),
    ean: List[str] = Form([]),
    descripcion: List[str] = Form([]),
    definicion: List[str] = Form([]),
    variable_x: str = Form("Razón social"),
    variable_y: str = Form("count"),
    tipo_grafico: str = Form("barras")
):
    global df_resultado_global
    if df_resultado_global.empty:
        return templates.TemplateResponse("index.html", {"request": request, "tabla": "<p>No hay datos cargados.</p>"})

    df_filtrado = df_resultado_global.copy()
    if proveedor:
        df_filtrado = df_filtrado[df_filtrado["Razón social"].isin(proveedor)]
    if ean:
        df_filtrado = df_filtrado[df_filtrado["EAN"].astype(str).isin(ean)]
    if descripcion:
        df_filtrado = df_filtrado[df_filtrado["Descripción"].isin(descripcion)]
    if definicion and "Definición equipo calidad" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["Definición equipo calidad"].isin(definicion)]

    tabla_html = df_filtrado.head(50).to_html(classes="table table-striped", index=False)
    grafico_html = crear_grafico(df_filtrado, tipo_grafico, variable_x, variable_y)
    grafico_secundario = crear_grafico_top_productos(df_filtrado)

    proveedores = sorted(df_resultado_global["Razón social"].dropna().unique().tolist())
    eans = sorted(df_resultado_global["EAN"].dropna().unique().astype(str).tolist())
    productos = sorted(df_resultado_global["Descripción"].dropna().unique().tolist())
    definiciones = sorted(df_resultado_global["Definición equipo calidad"].dropna().unique().tolist()) if "Definición equipo calidad" in df_resultado_global.columns else []
    columnas = sorted(df_resultado_global.columns)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "tabla": tabla_html,
        "grafico_html": grafico_html,
        "grafico_secundario": grafico_secundario,
        "descarga_disponible": True,
        "proveedores": proveedores,
        "eans": eans,
        "productos": productos,
        "definiciones": definiciones,
        "columnas": columnas,
        "filtros": {"proveedor": proveedor, "ean": ean, "descripcion": descripcion, "definicion": definicion},
        "tipo_grafico": tipo_grafico,
        "variable_x": variable_x,
        "variable_y": variable_y
    })


@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx",
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return {"error": "No hay informe disponible."}


def crear_grafico(df, tipo_grafico="barras", variable_x="Razón social", variable_y="count"):
    if df.empty or variable_x not in df.columns:
        return "<p>No hay datos suficientes para generar el gráfico.</p>"

    if variable_y == "count":
        df_chart = df[variable_x].value_counts().reset_index()
        df_chart.columns = [variable_x, "Cantidad"]
    else:
        df_chart = df.groupby(variable_x)[variable_y].count().reset_index()
        df_chart.rename(columns={variable_y: "Cantidad"}, inplace=True)

    if tipo_grafico == "barras":
        fig = px.bar(df_chart, x=variable_x, y="Cantidad", title=f"Cantidad por {variable_x}")
    elif tipo_grafico == "linea":
        fig = px.line(df_chart, x=variable_x, y="Cantidad", title=f"Evolución de {variable_y} por {variable_x}")
    elif tipo_grafico == "pie":
        fig = px.pie(df_chart, names=variable_x, values="Cantidad", title=f"Distribución de {variable_x}", hole=0.3)
    else:
        return "<p>Tipo de gráfico no reconocido.</p>"

    return fig.to_html(full_html=False)


def crear_grafico_top_productos(df):
    if "Descripción" not in df.columns or df.empty:
        return "<p>No hay datos suficientes para el gráfico de productos.</p>"

    top = df["Descripción"].value_counts().head(10).reset_index()
    top.columns = ["Producto", "Cantidad"]

    fig = px.bar(top, x="Producto", y="Cantidad", title="Top 10 productos con más reclamos", color="Cantidad")
    return fig.to_html(full_html=False)
