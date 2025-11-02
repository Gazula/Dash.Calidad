# ===============================
# main.py — versión local sin Drive
# ===============================

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import plotly.express as px
import os

# Crear aplicación
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# -------------------------------
# CONFIGURACIÓN DE RUTAS LOCALES
# -------------------------------
BASE_DATOS_PATH = "bases/Base de datos.xlsx"
RECLAMOS_PATH = "bases/Reclamos Ene-Sep 2025.xlsx"

# -------------------------------
# FUNCIÓN PARA CARGAR LOS DATOS
# -------------------------------
def cargar_datos_local():
    try:
        if not os.path.exists(BASE_DATOS_PATH):
            raise FileNotFoundError(f"No se encontró el archivo {BASE_DATOS_PATH}")
        if not os.path.exists(RECLAMOS_PATH):
            raise FileNotFoundError(f"No se encontró el archivo {RECLAMOS_PATH}")

        df_base = pd.read_excel(BASE_DATOS_PATH)
        df_reclamos = pd.read_excel(RECLAMOS_PATH)

        # Unificar columnas de reclamos con base de datos
        df = pd.merge(df_reclamos, df_base, on="EAN", how="left")

        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip()

        # Aseguramos que las columnas necesarias existan
        columnas_necesarias = [
            "Número del caso",
            "Fecha/hora de apertura",
            "Código de sucursal",
            "Respuesta tienda",
            "Definición equipo calidad",
            "Estado",
            "EAN",
            "Categoría",
            "Lote nro.",
            "Fecha de vencimiento",
            "Descripción",
            "Razón social"
        ]
        faltantes = [c for c in columnas_necesarias if c not in df.columns]
        if faltantes:
            raise KeyError(f"Faltan las columnas: {faltantes}")

        # Separar fecha y hora
        if "Fecha/hora de apertura" in df.columns:
            df["Fecha apertura"] = pd.to_datetime(df["Fecha/hora de apertura"], errors="coerce").dt.date
            df["Hora apertura"] = pd.to_datetime(df["Fecha/hora de apertura"], errors="coerce").dt.time

        return df

    except Exception as e:
        print(f"❌ Error al cargar datos locales: {e}")
        return pd.DataFrame()  # Devuelve vacío si hay error


# -------------------------------
# FUNCIÓN PARA DETECTAR ALERTAS
# -------------------------------
def detectar_alertas(df):
    if df.empty:
        return pd.DataFrame()

    # Detectar EAN + Lote con reclamos en múltiples tiendas
    conteo = (
        df.groupby(["EAN", "Lote nro."])["Código de sucursal"]
        .nunique()
        .reset_index()
        .rename(columns={"Código de sucursal": "Cantidad_tiendas"})
    )

    # Marcar tipo de alerta
    conteo["Tipo"] = conteo["Cantidad_tiendas"].apply(
        lambda x: "Aviso" if x == 2 else ("Alerta" if x >= 3 else "")
    )

    return conteo[conteo["Tipo"] != ""]


# -------------------------------
# RUTA PRINCIPAL — DASHBOARD
# -------------------------------
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = cargar_datos_local()

    if df.empty:
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "error_message": "No se pudieron cargar los datos locales."}
        )

    df_alertas = detectar_alertas(df)

    # =====================
    #     GRÁFICO 1 — Proveedores con más reclamos
    # =====================
    try:
        top_proveedores = (
            df["Razón social"]
            .value_counts()
            .head(10)
            .reset_index()
            .rename(columns={"index": "Proveedor", "Razón social": "Cantidad"})
        )
        graf_proveedores = px.bar(
            top_proveedores,
            x="Proveedor",
            y="Cantidad",
            title="Top 10 Proveedores con más reclamos",
            labels={"Proveedor": "Proveedor", "Cantidad": "Cantidad de reclamos"},
            text="Cantidad"
        ).to_html(full_html=False)
    except Exception as e:
        graf_proveedores = f"<p>Error generando gráfico de proveedores: {e}</p>"

    # =====================
    #     GRÁFICO 2 — Productos más reclamados
    # =====================
    try:
        top_productos = (
            df["Descripción"]
            .value_counts()
            .head(10)
            .reset_index()
            .rename(columns={"index": "Producto", "Descripción": "Cantidad"})
        )
        graf_productos = px.bar(
            top_productos,
            x="Producto",
            y="Cantidad",
            title="Top 10 Productos más reclamados",
            labels={"Producto": "Producto", "Cantidad": "Cantidad de reclamos"},
            text="Cantidad"
        ).to_html(full_html=False)
    except Exception as e:
        graf_productos = f"<p>Error generando gráfico de productos: {e}</p>"

    # =====================
    #     GRÁFICO 3 — Alertas
    # =====================
    try:
        graf_alertas = px.bar(
            df_alertas,
            x="EAN",
            y="Cantidad_tiendas",
            color="Tipo",
            title="Alertas detectadas (EAN + Lote con reclamos en múltiples tiendas)",
            labels={"EAN": "Código EAN", "Cantidad_tiendas": "Cantidad de tiendas"}
        ).to_html(full_html=False)
    except Exception as e:
        graf_alertas = f"<p>Error generando gráfico de alertas: {e}</p>"

    # =====================
    #     KPI NUMÉRICOS
    # =====================
    total_reclamos = len(df)
    total_proveedores = df["Razón social"].nunique()
    total_productos = df["Descripción"].nunique()
    total_avisos = len(df_alertas[df_alertas["Tipo"] == "Aviso"])
    total_alertas = len(df_alertas[df_alertas["Tipo"] == "Alerta"])

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "graf_proveedores": graf_proveedores,
            "graf_productos": graf_productos,
            "graf_alertas": graf_alertas,
            "total_reclamos": total_reclamos,
            "total_proveedores": total_proveedores,
            "total_productos": total_productos,
            "total_avisos": total_avisos,
            "total_alertas": total_alertas,
            "error_message": None
        }
    )


# -------------------------------
# EJECUCIÓN LOCAL
# -------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
