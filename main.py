from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import plotly.express as px
import gdown
import os

# --- Configuración base ---
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- IDs de archivos en Google Drive ---
# ⚠️ Reemplazá estos IDs por los tuyos
ID_BASE = "1Hp77ACYzKZnF3azHjuFrOTB_EaqvrUn5rXWjBYcOKcg"
ID_RECLAMOS = "1M262Vlx7KvBll3jzr9XYIN6iOPayDNfK_s_8RzipF4Y"

def descargar_archivos_desde_drive():
    """Descarga automáticamente los archivos más recientes desde Google Drive."""
    os.makedirs("bases", exist_ok=True)
    url_base = f"https://drive.google.com/uc?id={ID_BASE}"
    url_reclamos = f"https://drive.google.com/uc?id={ID_RECLAMOS}"

    print("Descargando Base de datos...")
    gdown.download(url_base, "bases/Base de datos.xlsx", quiet=False)
    print("Descargando Reclamos...")
    gdown.download(url_reclamos, "Reclamos.xlsx", quiet=False)

# --- Columnas de interés ---
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

# --- Función principal de procesamiento ---
def procesar_datos():
    descargar_archivos_desde_drive()

    df_base = pd.read_excel("bases/Base de datos.xlsx")
    df_reclamos = pd.read_excel("Reclamos.xlsx")

    # Normalizar nombres
    df_base.columns = [c.strip().lower() for c in df_base.columns]
    df_reclamos.columns = [c.strip().lower() for c in df_reclamos.columns]

    # Merge por EAN
    df = df_reclamos.merge(df_base, on="ean", how="left", suffixes=("", "_base"))

    # Separar fecha y hora
    if "fecha/hora de apertura" in df.columns:
        df["fecha/hora de apertura"] = pd.to_datetime(df["fecha/hora de apertura"], errors="coerce")
        df["Fecha de apertura"] = df["fecha/hora de apertura"].dt.date
        df["Hora de apertura"] = df["fecha/hora de apertura"].dt.time
    else:
        df["Fecha de apertura"] = None
        df["Hora de apertura"] = None

    # Ajustar mayúsculas exactas
    renombres = {
        "número del caso": "Número del caso",
        "codigo de sucursal": "Código de sucursal",
        "respuesta tienda": "Respuesta tienda",
        "definición equipo calidad": "Definición equipo calidad",
        "estado": "Estado",
        "ean": "EAN",
        "categoría": "Categoría",
        "lote nro.": "Lote nro.",
        "fecha de vencimiento": "Fecha de vencimiento",
        "descripcion": "Descripción",
        "razon_social": "Razón social",
    }
    df.rename(columns=renombres, inplace=True)

    # Filtrar columnas de interés
    columnas_finales = [c for c in COLUMNAS_INFORME if c in df.columns]
    df = df[columnas_finales]

    return df

# --- Detección de avisos y alertas ---
def detectar_alertas(df):
    if "EAN" not in df.columns or "Lote nro." not in df.columns or "Código de sucursal" not in df.columns:
        return pd.DataFrame(columns=["EAN", "Lote nro.", "Cantidad_tiendas", "Tipo"])

    df_alertas = (
        df.groupby(["EAN", "Lote nro."])["Código de sucursal"]
        .nunique()
        .reset_index(name="Cantidad_tiendas")
    )
    df_alertas["Tipo"] = df_alertas["Cantidad_tiendas"].apply(
        lambda x: "🚨 Alerta" if x >= 3 else ("⚠️ Aviso" if x == 2 else "")
    )
    df_alertas = df_alertas[df_alertas["Tipo"] != ""]
    return df_alertas

# --- Página principal (Dashboard) ---
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = procesar_datos()
    df_alertas = detectar_alertas(df)

    # --- KPIs ---
    total_reclamos = len(df)
    total_avisos = df_alertas[df_alertas["Tipo"] == "⚠️ Aviso"].shape[0]
    total_alertas = df_alertas[df_alertas["Tipo"] == "🚨 Alerta"].shape[0]
    top_proveedor = df["Razón social"].value_counts().idxmax() if not df["Razón social"].isna().all() else "-"
    top_producto = df["Descripción"].value_counts().idxmax() if not df["Descripción"].isna().all() else "-"

    # --- Gráficos ---
    graf_proveedores = px.bar(
        df["Razón social"].value_counts().head(10).reset_index(),
        x="index", y="Razón social",
        title="Top 10 Proveedores con más reclamos",
        labels={"index": "Proveedor", "Razón social": "Cantidad de reclamos"}
    ).to_html(full_html=False)

    graf_productos = px.bar(
        df["Descripción"].value_counts().head(10).reset_index(),
        x="index", y="Descripción",
        title="Top 10 Productos más reclamados",
        labels={"index": "Producto", "Descripción": "Cantidad de reclamos"}
    ).to_html(full_html=False)

    graf_alertas = px.bar(
        df_alertas, x="EAN", y="Cantidad_tiendas", color="Tipo",
        title="Alertas detectadas (EAN + Lote con reclamos en múltiples tiendas)"
    ).to_html(full_html=False)

    # --- Enviar todo al template ---
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": total_reclamos,
        "total_avisos": total_avisos,
        "total_alertas": total_alertas,
        "top_proveedor": top_proveedor,
        "top_producto": top_producto,
        "graf_proveedores": graf_proveedores,
        "graf_productos": graf_productos,
        "graf_alertas": graf_alertas,
        "alertas_tabla": df_alertas.head(20).to_html(classes="table table-striped", index=False)
    })



