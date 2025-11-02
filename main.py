from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import plotly.express as px
import os

# ==========================
# CONFIGURACIÓN INICIAL
# ==========================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Rutas locales de los archivos
BASE_PATH = "bases/Base de datos.xlsx"
RECLAMOS_PATH = "bases/Reclamos Ene-Sep 2025.xlsx"

# ==========================
# FUNCIÓN PARA NORMALIZAR NOMBRES DE COLUMNAS
# ==========================
def normalizar_col(col):
    col = str(col).strip().lower()
    col = col.replace(" ", "").replace("_", "")
    col = col.replace("ó", "o").replace("á", "a").replace("é", "e").replace("í", "i").replace("ú", "u")
    return col

def buscar_col(df, posibles):
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        if normalizar_col(p) in df_cols:
            return df_cols[normalizar_col(p)]
    return None

# ==========================
# CARGAR DATOS Y VALIDAR COLUMNAS
# ==========================
def cargar_datos_locales():
    try:
        df_base = pd.read_excel(BASE_PATH)
        df_reclamos = pd.read_excel(RECLAMOS_PATH)

        # Definimos las columnas esperadas (con variaciones aceptadas)
        columnas_requeridas = {
            "fecha_hora_apertura": ["fecha/hora de apertura", "fecha apertura", "fecha", "fecha y hora"],
            "codigo_sucursal": ["codigo de sucursal", "cod. sucursal", "sucursal"],
            "respuesta_tienda": ["respuesta tienda", "respuesta local", "respuesta"],
            "definicion_calidad": ["definicion equipo calidad", "definicion calidad", "equipo calidad"],
            "estado": ["estado", "situacion"],
            "ean": ["ean", "codigo ean", "cod ean"],
            "categoria": ["categoria", "rubro"],
            "lote_nro": ["lote nro.", "lote", "nro lote"],
            "fecha_vencimiento": ["fecha de vencimiento", "vencimiento", "fecha venc"],
            "descripcion": ["descripcion", "nombre producto", "producto"],
            "razon_social": ["razon social", "proveedor", "fabricante"]
        }

        # Mapeo automático
        mapeo = {}
        for key, posibles in columnas_requeridas.items():
            col_encontrada = buscar_col(df_reclamos, posibles)
            if col_encontrada:
                mapeo[key] = col_encontrada
            else:
                print(f"⚠️ No se encontró la columna esperada para: {key}")

        # Validar si hay columnas faltantes
        faltantes = [k for k, v in mapeo.items() if v is None]
        if faltantes:
            raise ValueError(f"Faltan las columnas: {faltantes}")

        # Aplicar mapeo
        df_reclamos = df_reclamos.rename(columns={v: k for k, v in mapeo.items()})
        df_base = df_base.rename(columns=lambda x: x.strip())

        # Unir con la base por EAN
        if "ean" in df_reclamos.columns and "EAN" in df_base.columns:
            df = df_reclamos.merge(df_base, left_on="ean", right_on="EAN", how="left")
        else:
            raise ValueError("No se encontró la columna 'EAN' en la base de datos o en reclamos")

        # Separar fecha y hora
        if "fecha_hora_apertura" in df.columns:
            df["Fecha"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.date
            df["Hora"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.time

        # Reordenar columnas según el formato deseado
        columnas_finales = [
            "numero del caso" if "numero del caso" in df.columns else None,
            "Fecha",
            "Hora",
            "codigo_sucursal",
            "respuesta_tienda",
            "definicion_calidad",
            "estado",
            "ean",
            "categoria",
            "lote_nro",
            "fecha_vencimiento",
            "descripcion",
            "razon_social"
        ]
        columnas_finales = [c for c in columnas_finales if c in df.columns]
        df = df[columnas_finales]

        return df

    except Exception as e:
        print(f"❌ Error al cargar datos locales: {e}")
        return pd.DataFrame()

# ==========================
# DASHBOARD PRINCIPAL
# ==========================
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = cargar_datos_locales()
    if df.empty:
        return templates.TemplateResponse("dashboard.html", {"request": request, "error": "Error al cargar datos."})

    # =======================
    # Gráfico 1 - Proveedores
    # =======================
    graf_proveedores = px.bar(
        df["razon_social"].value_counts().head(10).reset_index(),
        x="index", y="razon_social",
        title="Top 10 Proveedores con más reclamos",
        labels={"index": "Proveedor", "razon_social": "Cantidad de reclamos"}
    ).to_html(full_html=False)

    # =======================
    # Gráfico 2 - Productos
    # =======================
    graf_productos = px.bar(
        df["descripcion"].value_counts().head(10).reset_index(),
        x="index", y="descripcion",
        title="Top 10 Productos más reclamados",
        labels={"index": "Producto", "descripcion": "Cantidad de reclamos"}
    ).to_html(full_html=False)

    # =======================
    # Gráfico 3 - Alertas (EAN + Lote)
    # =======================
    df_alertas = (
        df.groupby(["ean", "lote_nro"])
        .agg({"codigo_sucursal": "nunique"})
        .reset_index()
        .rename(columns={"codigo_sucursal": "Cantidad_tiendas"})
    )
    df_alertas["Tipo"] = df_alertas["Cantidad_tiendas"].apply(lambda x: "⚠️ Alerta" if x >= 3 else "Aviso" if x == 2 else None)
    df_alertas = df_alertas.dropna(subset=["Tipo"])

    graf_alertas = px.bar(
        df_alertas, x="ean", y="Cantidad_tiendas", color="Tipo",
        title="Alertas detectadas (EAN + Lote con reclamos en múltiples tiendas)"
    ).to_html(full_html=False)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "graf_proveedores": graf_proveedores,
        "graf_productos": graf_productos,
        "graf_alertas": graf_alertas,
        "total_avisos": df_alertas[df_alertas["Tipo"] == "Aviso"].shape[0],
        "total_alertas": df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"].shape[0],
        "error": None
    })

