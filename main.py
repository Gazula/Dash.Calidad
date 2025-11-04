from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import plotly.express as px
import os
import unicodedata

# ==========================
# CONFIGURACIÓN INICIAL
# ==========================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Rutas locales
BASE_PATH = "bases/Base de datos.xlsx"
RECLAMOS_PATH = "bases/Reclamos Ene-Sep 2025.xlsx"

# ==========================
# FUNCIONES DE LIMPIEZA
# ==========================
def limpiar_texto(texto):
    texto = str(texto).strip().lower()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = texto.replace(".", "").replace("_", "").replace(" ", "")
    return texto

def buscar_col(df, posibles):
    df_cols = {limpiar_texto(c): c for c in df.columns}
    for p in posibles:
        key = limpiar_texto(p)
        if key in df_cols:
            return df_cols[key]
    return None

# ==========================
# CARGAR DATOS Y VALIDAR
# ==========================
def cargar_datos_locales():
    try:
        df_base = pd.read_excel(BASE_PATH)
        df_reclamos = pd.read_excel(RECLAMOS_PATH)

        columnas_requeridas = {
            "fecha_hora_apertura": ["fecha/hora de apertura", "fecha hora apertura", "fechaapertura"],
            "codigo_sucursal": ["codigo de sucursal", "codigosucursal"],
            "respuesta_tienda": ["respuesta tienda"],
            "definicion_calidad": ["definicion equipo calidad"],
            "estado": ["estado"],
            "ean": ["ean"],
            "categoria": ["categoria"],
            "lote_nro": ["lote nro", "lotenro"],
            "fecha_vencimiento": ["fecha de vencimiento"],
            "descripcion": ["descripcion", "producto", "nombreproducto"],
            "razon_social": ["razon social", "proveedor", "fabricante"]
        }

        mapeo = {}
        for key, posibles in columnas_requeridas.items():
            col_encontrada = buscar_col(df_reclamos, posibles)
            if col_encontrada:
                mapeo[key] = col_encontrada
            else:
                print(f"⚠️ No se encontró la columna esperada para: {key}")

        # Renombrar columnas que se encontraron
        df_reclamos = df_reclamos.rename(columns={v: k for k, v in mapeo.items()})
        df_base = df_base.rename(columns=lambda x: x.strip())

        # Unir con base si existe EAN
        if "ean" in df_reclamos.columns and "EAN" in df_base.columns:
            df = df_reclamos.merge(df_base, left_on="ean", right_on="EAN", how="left")
        else:
            df = df_reclamos.copy()

        # Separar fecha/hora
        if "fecha_hora_apertura" in df.columns:
            df["Fecha"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.date
            df["Hora"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.time

        return df

    except Exception as e:
        print(f"❌ Error al cargar datos locales: {e}")
        return pd.DataFrame()

# ==========================
# DASHBOARD
# ==========================
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = cargar_datos_locales()
    if df.empty:
        return templates.TemplateResponse("dashboard.html", {"request": request, "error": "Error al cargar datos."})

    # Total de reclamos
    total_reclamos = len(df)

    # --- Gráfico 1: Proveedores
    if "razon_social" in df.columns:
        graf_proveedores = px.bar(
            df["razon_social"].value_counts().head(10).reset_index().rename(columns={"index": "Proveedor", "razon_social": "Cantidad"}),
            x="Proveedor", y="Cantidad",
            title="Top 10 Proveedores con más reclamos"
        ).to_html(full_html=False)
        proveedor_top = df["razon_social"].mode()[0]
    else:
        graf_proveedores = "<p style='color:red;'>Columna 'razon_social' no disponible</p>"
        proveedor_top = "-"

    # --- Gráfico 2: Productos
    if "descripcion" in df.columns:
        graf_productos = px.bar(
            df["descripcion"].value_counts().head(10).reset_index().rename(columns={"index": "Producto", "descripcion": "Cantidad"}),
            x="Producto", y="Cantidad",
            title="Top 10 Productos más reclamados"
        ).to_html(full_html=False)
        producto_top = df["descripcion"].mode()[0]
    else:
        graf_productos = "<p style='color:red;'>Columna 'descripcion' no disponible</p>"
        producto_top = "-"

    # --- Gráfico 3: Alertas
    if all(c in df.columns for c in ["ean", "lote_nro", "codigo_sucursal"]):
        df_alertas = (
            df.groupby(["ean", "lote_nro"])
            .agg({"codigo_sucursal": "nunique"})
            .reset_index()
            .rename(columns={"codigo_sucursal": "Cantidad_tiendas"})
        )
        df_alertas["Tipo"] = df_alertas["Cantidad_tiendas"].apply(lambda x: "⚠️ Alerta" if x >= 3 else "Aviso" if x == 2 else None)
        df_alertas = df_alertas.dropna(subset=["Tipo"])
    else:
        df_alertas = pd.DataFrame(columns=["ean", "lote_nro", "Cantidad_tiendas", "Tipo"])

    graf_alertas = px.bar(
        df_alertas, x="ean", y="Cantidad_tiendas", color="Tipo",
        title="Alertas detectadas (EAN + Lote con reclamos en múltiples tiendas)"
    ).to_html(full_html=False)

    total_avisos = df_alertas[df_alertas["Tipo"] == "Aviso"].shape[0]
    total_alertas = df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"].shape[0]

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "graf_proveedores": graf_proveedores,
        "graf_productos": graf_productos,
        "graf_alertas": graf_alertas,
        "total_reclamos": total_reclamos,
        "total_avisos": total_avisos,
        "total_alertas": total_alertas,
        "proveedor_top": proveedor_top,
        "producto_top": producto_top,
        "error": None
    })


