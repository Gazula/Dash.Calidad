from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import plotly.express as px
import os

# ==========================
# CONFIGURACIÓN INICIAL
# ==========================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BASE_PATH = "bases/Base de datos.xlsx"
RECLAMOS_PATH = "bases/Reclamos Ene-Sep 2025.xlsx"

# ==========================
# FUNCIONES AUXILIARES
# ==========================
def normalizar_col(col):
    col = str(col).strip().lower()
    reemplazos = {"ó":"o","á":"a","é":"e","í":"i","ú":"u"}
    for k,v in reemplazos.items():
        col = col.replace(k, v)
    col = col.replace(" ", "").replace("_", "")
    return col

def buscar_col(df, posibles):
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        if normalizar_col(p) in df_cols:
            return df_cols[normalizar_col(p)]
    return None

# ==========================
# CARGA Y PREPROCESAMIENTO
# ==========================
def cargar_datos_locales():
    try:
        df_base = pd.read_excel(BASE_PATH)
        df_reclamos = pd.read_excel(RECLAMOS_PATH)

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
            "descripcion": ["descripcion", "nombre producto", "producto", "articulo"],
            "razon_social": ["razon social", "proveedor", "fabricante", "empresa"]
        }

        # --- Detección dinámica ---
        mapeo = {}
        for key, posibles in columnas_requeridas.items():
            col_encontrada = buscar_col(df_reclamos, posibles)
            if not col_encontrada:
                col_encontrada = buscar_col(df_base, posibles)
            if col_encontrada:
                mapeo[key] = col_encontrada
            else:
                print(f"⚠️ No se encontró la columna esperada para: {key}")

        # Renombrar columnas encontradas
        for key, col in mapeo.items():
            if col in df_reclamos.columns:
                df_reclamos = df_reclamos.rename(columns={col: key})
            elif col in df_base.columns:
                df_base = df_base.rename(columns={col: key})

        # --- Unir bases ---
        if "ean" in df_reclamos.columns and "ean" in df_base.columns:
            df = df_reclamos.merge(df_base, on="ean", how="left")
        elif "ean" in df_reclamos.columns and "EAN" in df_base.columns:
            df = df_reclamos.merge(df_base, left_on="ean", right_on="EAN", how="left")
        else:
            raise ValueError("No se encontró la columna 'EAN' en los archivos.")

        # --- Fecha y hora ---
        if "fecha_hora_apertura" in df.columns:
            df["Fecha"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.date
            df["Hora"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.time

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

    total_reclamos = len(df)

    # =======================
    # LISTADOS DE ALERTAS Y AVISOS
    # =======================
    df_alertas = pd.DataFrame()
    if "ean" in df.columns and "lote_nro" in df.columns:
        df_alertas = (
            df.groupby(["ean", "lote_nro"])
            .agg({"codigo_sucursal": "nunique"})
            .reset_index()
            .rename(columns={"codigo_sucursal": "Cantidad_tiendas"})
        )
        df_alertas["Tipo"] = df_alertas["Cantidad_tiendas"].apply(
            lambda x: "⚠️ Alerta" if x >= 3 else "Aviso" if x == 2 else None
        )
        df_alertas = df_alertas.dropna(subset=["Tipo"])

    total_alertas = df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"].shape[0]
    total_avisos = df_alertas[df_alertas["Tipo"] == "Aviso"].shape[0]

    # =======================
    # TOP PROVEEDORES
    # =======================
    graf_proveedores = None
    if "razon_social" in df.columns:
        top_prov = df["razon_social"].value_counts().head(10).reset_index()
        top_prov.columns = ["Proveedor", "Cantidad"]
        graf_proveedores = px.bar(
            top_prov, x="Proveedor", y="Cantidad",
            title="Top 10 Proveedores con más reclamos"
        ).to_html(full_html=False)
    else:
        graf_proveedores = "<p style='color:red'>No se encontraron datos de proveedores.</p>"

    # =======================
    # TOP PRODUCTOS
    # =======================
    graf_productos = None
    if "descripcion" in df.columns:
        top_prod = df["descripcion"].value_counts().head(10).reset_index()
        top_prod.columns = ["Producto", "Cantidad"]
        graf_productos = px.bar(
            top_prod, x="Producto", y="Cantidad",
            title="Top 10 Productos más reclamados"
        ).to_html(full_html=False)
    else:
        graf_productos = "<p style='color:red'>No se encontraron datos de productos.</p>"

    # =======================
    # CREACIÓN DE LISTADOS
    # =======================
    listado_avisos = df_alertas[df_alertas["Tipo"] == "Aviso"][["ean", "lote_nro", "Cantidad_tiendas"]].to_dict(orient="records")
    listado_alertas = df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"][["ean", "lote_nro", "Cantidad_tiendas"]].to_dict(orient="records")

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "graf_proveedores": graf_proveedores,
        "graf_productos": graf_productos,
        "listado_avisos": listado_avisos,
        "listado_alertas": listado_alertas,
        "total_reclamos": total_reclamos,
        "total_avisos": total_avisos,
        "total_alertas": total_alertas,
        "error": None
    })
