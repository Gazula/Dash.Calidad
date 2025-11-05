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

# Rutas locales
BASE_PATH = "bases/Base de datos.xlsx"
RECLAMOS_PATH = "bases/Reclamos Ene-Sep 2025.xlsx"

# ==========================
# FUNCIONES AUXILIARES
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
            "descripcion": ["descripcion", "nombre producto", "producto"],
            "razon_social": ["razon social", "proveedor", "fabricante"]
        }

        # Intentar mapear columnas dinámicamente
        mapeo = {}
        for key, posibles in columnas_requeridas.items():
            col = buscar_col(df_reclamos, posibles)
            if col:
                mapeo[key] = col
            else:
                print(f"⚠️ No se encontró la columna esperada para: {key}")

        df_reclamos = df_reclamos.rename(columns={v: k for k, v in mapeo.items()})
        df_base = df_base.rename(columns=lambda x: x.strip())

        # Si no existen columnas, intentar extraer del otro archivo
        if "descripcion" not in df_reclamos.columns and "Descripción" in df_base.columns:
            df_reclamos = df_reclamos.merge(df_base[["EAN", "Descripción"]], left_on="ean", right_on="EAN", how="left")
            df_reclamos["descripcion"] = df_reclamos["Descripción"]
        if "razon_social" not in df_reclamos.columns and "Razon_social" in df_base.columns:
            df_reclamos = df_reclamos.merge(df_base[["EAN", "Razon_social"]], left_on="ean", right_on="EAN", how="left")
            df_reclamos["razon_social"] = df_reclamos["Razon_social"]

        # Separar fecha y hora si aplica
        if "fecha_hora_apertura" in df_reclamos.columns:
            df_reclamos["Fecha"] = pd.to_datetime(df_reclamos["fecha_hora_apertura"], errors="coerce").dt.date
            df_reclamos["Hora"] = pd.to_datetime(df_reclamos["fecha_hora_apertura"], errors="coerce").dt.time

        return df_reclamos

    except Exception as e:
        print(f"❌ Error al cargar datos locales: {e}")
        return pd.DataFrame()

def generar_alertas(df):
    if df.empty or "ean" not in df.columns or "lote_nro" not in df.columns:
        return pd.DataFrame(columns=["ean", "lote_nro", "Cantidad_tiendas", "Tipo"])

    df_alertas = (
        df.groupby(["ean", "lote_nro"])
        .agg({"codigo_sucursal": "nunique"})
        .reset_index()
        .rename(columns={"codigo_sucursal": "Cantidad_tiendas"})
    )

    df_alertas["Tipo"] = df_alertas["Cantidad_tiendas"].apply(
        lambda x: "⚠️ Alerta" if x >= 3 else "Aviso" if x == 2 else None
    )
    return df_alertas.dropna(subset=["Tipo"])

# ==========================
# DASHBOARD PRINCIPAL
# ==========================
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = cargar_datos_locales()
    if df.empty:
        return templates.TemplateResponse("dashboard.html", {"request": request, "error": "Error al cargar datos."})

    df_alertas = generar_alertas(df)

    # === Gráfico 1: Proveedores ===
    graf_proveedores = "<p>No se encontraron datos de proveedores.</p>"
    if "razon_social" in df.columns and df["razon_social"].notna().any():
        top_prov = df["razon_social"].value_counts().head(10).reset_index()
        top_prov.columns = ["Proveedor", "Cantidad"]
        graf_proveedores = px.bar(
            top_prov, x="Proveedor", y="Cantidad",
            title="Top 10 Proveedores con más reclamos"
        ).to_html(full_html=False)

    # === Gráfico 2: Productos ===
    graf_productos = "<p>No se encontraron datos de productos.</p>"
    if "descripcion" in df.columns and df["descripcion"].notna().any():
        top_prod = df["descripcion"].value_counts().head(10).reset_index()
        top_prod.columns = ["Producto", "Cantidad"]
        graf_productos = px.bar(
            top_prod, x="Producto", y="Cantidad",
            title="Top 10 Productos más reclamados"
        ).to_html(full_html=False)

    # === Listado de alertas ===
    df_alertas_listado = df_alertas.sort_values(by="Cantidad_tiendas", ascending=False).head(20)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "graf_proveedores": graf_proveedores,
        "graf_productos": graf_productos,
        "alertas_listado": df_alertas_listado.to_dict(orient="records"),
        "total_avisos": df_alertas[df_alertas["Tipo"] == "Aviso"].shape[0],
        "total_alertas": df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"].shape[0],
        "error": None
    })

# ==========================
# RUTA DETALLE
# ==========================
@app.get("/avisos", response_class=HTMLResponse)
async def ver_avisos(request: Request):
    df = cargar_datos_locales()
    df_alertas = generar_alertas(df)
    df_filtrado = df_alertas[df_alertas["Tipo"] == "Aviso"]
    return templates.TemplateResponse("detalle.html", {
        "request": request,
        "titulo": "Avisos detectados",
        "tipo": "Aviso",
        "data": df_filtrado.to_dict(orient="records")
    })

@app.get("/alertas", response_class=HTMLResponse)
async def ver_alertas(request: Request):
    df = cargar_datos_locales()
    df_alertas = generar_alertas(df)
    df_filtrado = df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"]
    return templates.TemplateResponse("detalle.html", {
        "request": request,
        "titulo": "Alertas detectadas",
        "tipo": "⚠️ Alerta",
        "data": df_filtrado.to_dict(orient="records")
    })
