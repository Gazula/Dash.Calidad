from fastapi import FastAPI, UploadFile, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import os
import io

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

RESULT_PATH = "informe_resultado.xlsx"
BASE_PATH = "bases/Base de datos.xlsx"

df_resultado_global = pd.DataFrame()

# ======================================================
# Normalización de nombres de columnas
# ======================================================
def normalizar_col(col):
    c = str(col).strip().lower()
    c = c.replace(" ", "").replace("_", "")
    c = (
        c.replace("á", "a")
         .replace("é", "e")
         .replace("í", "i")
         .replace("ó", "o")
         .replace("ú", "u")
    )
    return c


def buscar_col(df, posibles):
    df_cols = {normalizar_col(c): c for c in df.columns}
    for p in posibles:
        key = normalizar_col(p)
        if key in df_cols:
            return df_cols[key]
    return None


# ======================================================
# Helpers
# ======================================================
def preparar_df_base(df_reclamos: pd.DataFrame) -> pd.DataFrame:
    """
    Une el archivo de reclamos con la base por EAN,
    agregando proveedor y descripción de la base si existen.
    """
    df_r = df_reclamos.copy()
    df_r.columns = [str(c).strip() for c in df_r.columns]

    if not os.path.exists(BASE_PATH):
        # Si no existe la base, seguimos solo con reclamos
        return df_r

    df_b = pd.read_excel(BASE_PATH)
    df_b.columns = [str(c).strip() for c in df_b.columns]

    col_ean_r = buscar_col(df_r, ["ean", "codigo ean", "cod ean"])
    col_ean_b = buscar_col(df_b, ["ean", "codigo ean", "cod ean"])
    col_prov_b = buscar_col(df_b, ["razon social", "razón social", "proveedor"])
    col_desc_b = buscar_col(df_b, ["descripcion", "descripción", "producto", "nombre producto"])

    if not col_ean_r or not col_ean_b:
        # No se pudo mapear EAN, devolvemos solo reclamos
        return df_r

    # Normalizar EAN como texto
    df_r[col_ean_r] = df_r[col_ean_r].astype(str).str.strip()
    df_b[col_ean_b] = df_b[col_ean_b].astype(str).str.strip()

    cols_keep = [col_ean_b]
    if col_prov_b:
        cols_keep.append(col_prov_b)
    if col_desc_b:
        cols_keep.append(col_desc_b)

    df_b_red = df_b[cols_keep].drop_duplicates()

    rename_map = {col_ean_b: "EAN_base"}
    if col_prov_b:
        rename_map[col_prov_b] = "proveedor_base"
    if col_desc_b:
        rename_map[col_desc_b] = "descripcion_base"

    df_b_red = df_b_red.rename(columns=rename_map)

    df_merge = df_r.merge(
        df_b_red,
        left_on=col_ean_r,
        right_on="EAN_base",
        how="left"
    )

    return df_merge


def preparar_df_analisis(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    A partir del df_resultado_global, unifica nombres de columnas
    en columnas canónicas para análisis:
    - fecha_analisis
    - mes_analisis
    - ean
    - lote
    - descripcion_analisis
    - proveedor_analisis
    - tienda_analisis
    - subtipo_analisis
    - calidad_analisis
    """
    df = df_in.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Mapear columnas origen
    col_fecha = buscar_col(df, ["fecha/hora de apertura", "fecha de apertura", "fecha"])
    col_ean = buscar_col(df, ["ean", "codigo ean", "cod ean"])
    col_lote = buscar_col(df, ["lote nro.", "lote", "nro lote"])
    col_desc_reclamo = buscar_col(df, ["descripcion"])
    col_desc_base = buscar_col(df, ["descripcion_base"])
    col_prov = buscar_col(df, ["proveedor_base", "razon social", "razón social", "proveedor"])
    col_tienda = buscar_col(df, ["codigo de sucursal", "codigo_sucursal", "sucursal", "tienda"])
    col_subtipo = buscar_col(df, ["sub tipo caso", "subtipo", "tipificacion", "tipificación", "tipo problema"])
    col_calidad = buscar_col(df, ["definicion equipo calidad", "definición equipo calidad", "definicion calidad", "calidad"])

    # EAN
    if col_ean:
        df["ean"] = df[col_ean].astype(str).str.strip()
    else:
        df["ean"] = ""

    # Lote
    if col_lote:
        df["lote"] = df[col_lote].astype(str).str.strip()
    else:
        df["lote"] = ""

    # Descripción: preferimos la de base, luego la del reclamo
    if col_desc_base:
        desc_base = df[col_desc_base].astype(str)
    else:
        desc_base = pd.Series([""] * len(df))

    if col_desc_reclamo:
        desc_reclamo = df[col_desc_reclamo].astype(str)
    else:
        desc_reclamo = pd.Series([""] * len(df))

    descripcion_final = desc_base.where(desc_base != "", desc_reclamo)
    df["descripcion_analisis"] = descripcion_final.fillna("")

    # Proveedor
    if col_prov:
        df["proveedor_analisis"] = df[col_prov].astype(str).fillna("")
    else:
        df["proveedor_analisis"] = "No tipificado"

    # Tienda
    if col_tienda:
        df["tienda_analisis"] = df[col_tienda].astype(str).fillna("")
    else:
        df["tienda_analisis"] = ""

    # Subtipo
    if col_subtipo:
        df["subtipo_analisis"] = df[col_subtipo].astype(str).fillna("")
    else:
        df["subtipo_analisis"] = ""

    # Calidad
    if col_calidad:
        df["calidad_analisis"] = df[col_calidad].astype(str).fillna("")
    else:
        df["calidad_analisis"] = ""

    # Fecha y Mes
    if col_fecha:
        fechas = pd.to_datetime(df[col_fecha], errors="coerce")
        df["fecha_analisis"] = fechas
        meses_num = fechas.dt.month
        mapa_meses = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        df["mes_analisis"] = meses_num.map(mapa_meses).fillna("Sin mes")
    else:
        df["fecha_analisis"] = pd.NaT
        df["mes_analisis"] = "Sin mes"

    # Aseguramos todo en string donde corresponde
    for col in ["ean", "lote", "descripcion_analisis", "proveedor_analisis",
                "tienda_analisis", "subtipo_analisis", "calidad_analisis", "mes_analisis"]:
        df[col] = df[col].fillna("").astype(str)

    return df


# ======================================================
# Página inicial
# ======================================================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": os.path.exists(RESULT_PATH)
    })


# ======================================================
# Procesar archivo cargado
# ======================================================
@app.post("/analizar", response_class=HTMLResponse)
async def analizar(request: Request, file: UploadFile):
    global df_resultado_global

    contenido = await file.read()
    df_reclamos = pd.read_excel(io.BytesIO(contenido))

    # Une con base por EAN (si existe)
    df_merge = preparar_df_base(df_reclamos)

    # Guardamos en global y en Excel
    df_resultado_global = df_merge.copy()
    df_merge.to_excel(RESULT_PATH, index=False)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "descarga_disponible": True
    })


# ======================================================
# DASHBOARD (solo reclamos con lote)
# ======================================================
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    mes: str = "",
    subtipo: str = "",
    calidad: str = "",
    tienda: str = ""
):
    global df_resultado_global

    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        df_resultado_global = pd.read_excel(RESULT_PATH)

    if df_resultado_global.empty:
        return HTMLResponse("<h3>No hay datos cargados. Primero subí un archivo.</h3>")

    df = preparar_df_analisis(df_resultado_global)

    # Trabajamos SOLO con reclamos con lote
    df_lotes = df[df["lote"] != ""].copy()

    # Aplicar filtros si vienen
    if mes:
        df_lotes = df_lotes[df_lotes["mes_analisis"] == mes]
    if subtipo:
        df_lotes = df_lotes[df_lotes["subtipo_analisis"] == subtipo]
    if calidad:
        df_lotes = df_lotes[df_lotes["calidad_analisis"] == calidad]
    if tienda:
        df_lotes = df_lotes[df_lotes["tienda_analisis"] == tienda]

    # Resumen por EAN + Lote
    if not df_lotes.empty:
        resumen = (
            df_lotes
            .groupby(["ean", "lote"])
            ["tienda_analisis"]
            .nunique()
            .reset_index(name="cantidad_tiendas")
        )
    else:
        resumen = pd.DataFrame(columns=["ean", "lote", "cantidad_tiendas"])

    avisos = resumen[resumen["cantidad_tiendas"] == 2].copy()
    alertas = resumen[resumen["cantidad_tiendas"] >= 3].copy()

    # Info adicional (producto, proveedor)
    info = df_lotes[["ean", "lote", "descripcion_analisis", "proveedor_analisis"]].drop_duplicates()

    if not avisos.empty:
        avisos = avisos.merge(info, on=["ean", "lote"], how="left")
    if not alertas.empty:
        alertas = alertas.merge(info, on=["ean", "lote"], how="left")

    # Renombrar columnas para usarlas directo en el HTML
    if not avisos.empty:
        avisos = avisos.rename(columns={
            "descripcion_analisis": "descripcion",
            "proveedor_analisis": "proveedor"
        })
    if not alertas.empty:
        alertas = alertas.rename(columns={
            "descripcion_analisis": "descripcion",
            "proveedor_analisis": "proveedor"
        })

    # Opciones de filtros (sobre reclamos con lote)
    filtros = {
        "meses": sorted(df_lotes["mes_analisis"].unique()),
        "subtipos": sorted(df_lotes["subtipo_analisis"].unique()),
        "calidades": sorted(df_lotes["calidad_analisis"].unique()),
        "tiendas": sorted(df_lotes["tienda_analisis"].unique())
    }

    seleccion = {
        "mes": mes,
        "subtipo": subtipo,
        "calidad": calidad,
        "tienda": tienda
    }

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_reclamos": len(df_lotes),
        "avisos": avisos.to_dict(orient="records"),
        "alertas": alertas.to_dict(orient="records"),
        "filtros": filtros,
        "seleccion": seleccion
    })


# ======================================================
# ANÁLISIS AVANZADO (todos los reclamos, incluidos sin lote)
# ======================================================
@app.get("/analisis", response_class=HTMLResponse)
async def analisis(request: Request):
    global df_resultado_global

    if df_resultado_global.empty and os.path.exists(RESULT_PATH):
        df_resultado_global = pd.read_excel(RESULT_PATH)

    if df_resultado_global.empty:
        return HTMLResponse("<h3>No hay datos cargados. Primero subí un archivo.</h3>")

    df = preparar_df_analisis(df_resultado_global)

    # Top productos, proveedores, tiendas (ignoramos vacíos)
    top_prod = (
        df[df["descripcion_analisis"] != ""]
        .groupby("descripcion_analisis")
        .size()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )

    top_prov = (
        df[df["proveedor_analisis"] != ""]
        .groupby("proveedor_analisis")
        .size()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )

    top_tiendas = (
        df[df["tienda_analisis"] != ""]
        .groupby("tienda_analisis")
        .size()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )

    reclamos_mes = (
        df[df["mes_analisis"] != ""]
        .groupby("mes_analisis")
        .size()
        .to_dict()
    )

    subtipos = (
        df[df["subtipo_analisis"] != ""]
        .groupby("subtipo_analisis")
        .size()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )

    return templates.TemplateResponse("analisis.html", {
        "request": request,
        "top_prod": top_prod,
        "top_prov": top_prov,
        "top_tiendas": top_tiendas,
        "reclamos_mes": reclamos_mes,
        "subtipos": subtipos
    })


# ======================================================
# DESCARGA DEL INFORME
# ======================================================
@app.get("/descargar")
async def descargar():
    if os.path.exists(RESULT_PATH):
        return FileResponse(RESULT_PATH, filename="Informe_EANs_Tipificados.xlsx")
    return {"error": "No existe archivo"}
