from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import plotly.express as px
import os
import unicodedata
from typing import Optional

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BASE_PATH = "bases/Base de datos.xlsx"
RECLAMOS_PATH = "bases/Reclamos Ene-Sep 2025.xlsx"

# -------------------------
# util: normalizar y buscar columnas tolerante
# -------------------------
def clean_key(s):
    s = str(s).strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    s = s.replace(" ", "").replace("_", "").replace(".", "")
    return s

def find_col(df, candidates):
    cols_map = {clean_key(c): c for c in df.columns}
    for cand in candidates:
        k = clean_key(cand)
        if k in cols_map:
            return cols_map[k]
    return None

# -------------------------
# carga y preparación de datos
# -------------------------
def cargar_datos_locales():
    try:
        df_base = pd.read_excel(BASE_PATH)
        df_reclamos = pd.read_excel(RECLAMOS_PATH)

        # columnas que intentamos encontrar (variantes)
        columnas = {
            "fecha_hora_apertura": ["fecha/hora de apertura", "fecha hora apertura", "fechaapertura", "fecha"],
            "codigo_sucursal": ["codigo de sucursal", "codigosucursal", "sucursal", "codigosuc"],
            "sub_tipo_caso": ["sub tipo caso", "subtipocaso", "sub_tipo_caso", "sub tipo"],
            "respuesta_tienda": ["respuesta tienda", "respuesta"],
            "definicion_calidad": ["definicion equipo calidad", "definicion calidad", "equipo calidad", "definicion_equipo_calidad"],
            "estado": ["estado", "situacion"],
            "ean": ["ean", "codigo ean", "codeean"],
            "categoria": ["categoria", "rubro"],
            "lote_nro": ["lote nro", "lotenro", "lote_nro", "nrolote"],
            "fecha_vencimiento": ["fecha de vencimiento", "vencimiento", "fechavencimiento"],
            "descripcion": ["descripcion", "nombre producto", "producto", "articulo"],
            "razon_social": ["razon social", "proveedor", "fabricante", "razonsocial", "empresa"]
        }

        # detectamos cols en ambos archivos (preferimos reclamos)
        m = {}
        for key, candidates in columnas.items():
            col = find_col(df_reclamos, candidates)
            if not col:
                col = find_col(df_base, candidates)
            if col:
                m[key] = col
            else:
                print(f"⚠️ No se encontró la columna esperada para: {key}")

        # renombrar (solo las columnas detectadas) en cada df según donde aparezcan
        for k, col_name in m.items():
            if col_name in df_reclamos.columns:
                df_reclamos = df_reclamos.rename(columns={col_name: k})
            if col_name in df_base.columns:
                df_base = df_base.rename(columns={col_name: k})

        # aseguramos que exista la columna EAN en ambos para merge (aceptamos EAN o ean)
        if "ean" not in df_reclamos.columns:
            # intentar encontrar 'EAN' mayúscula o variantes
            col_ean_reclamos = find_col(df_reclamos, ["EAN", "Codigo EAN", "Código EAN"])
            if col_ean_reclamos:
                df_reclamos = df_reclamos.rename(columns={col_ean_reclamos: "ean"})

        if "ean" not in df_base.columns:
            col_ean_base = find_col(df_base, ["EAN", "Codigo EAN", "Código EAN"])
            if col_ean_base:
                df_base = df_base.rename(columns={col_ean_base: "ean"})

        # Merge: preferimos merge on 'ean'
        if "ean" in df_reclamos.columns and "ean" in df_base.columns:
            df = df_reclamos.merge(df_base, on="ean", how="left", suffixes=("", "_base"))
        elif "ean" in df_reclamos.columns:
            df = df_reclamos.copy()
        else:
            raise ValueError("No se encontró la columna EAN en los archivos.")

        # Si descripcion/razon_social no están en df final, intentar traer de base
        if "descripcion" not in df.columns and "descripcion" in df_base.columns:
            df = df.merge(df_base[["ean", "descripcion"]].drop_duplicates(), on="ean", how="left")
        if "razon_social" not in df.columns and "razon_social" in df_base.columns:
            df = df.merge(df_base[["ean", "razon_social"]].drop_duplicates(), on="ean", how="left")

        # crear Fecha/Hora si existe fecha_hora_apertura
        if "fecha_hora_apertura" in df.columns:
            df["Fecha"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.date
            df["Hora"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.time
            # mes numérico (1-12) para filtro
            df["Mes"] = pd.to_datetime(df["fecha_hora_apertura"], errors="coerce").dt.month
        else:
            df["Fecha"] = pd.NaT
            df["Hora"] = pd.NaT
            df["Mes"] = pd.NA

        # normalizar nombres de columnas que puedan aparecer con sufijos
        # (por ejemplo 'razon_social_base' si existió conflicto)
        if "razon_social_base" in df.columns and "razon_social" not in df.columns:
            df = df.rename(columns={"razon_social_base": "razon_social"})
        if "descripcion_base" in df.columns and "descripcion" not in df.columns:
            df = df.rename(columns={"descripcion_base": "descripcion"})

        return df

    except Exception as e:
        print("❌ Error al cargar datos locales:", e)
        return pd.DataFrame()

# -------------------------
# generar alertas (avisos/alertas)
# -------------------------
def generar_alertas(df):
    if df.empty or "ean" not in df.columns or "lote_nro" not in df.columns or "codigo_sucursal" not in df.columns:
        return pd.DataFrame(columns=["ean", "lote_nro", "Cantidad_tiendas", "Tipo", "descripcion", "razon_social"])
    df_alertas = (
        df.groupby(["ean", "lote_nro"])
        .agg({"codigo_sucursal": "nunique", "descripcion": "first", "razon_social": "first"})
        .reset_index()
        .rename(columns={"codigo_sucursal": "Cantidad_tiendas"})
    )
    df_alertas["Tipo"] = df_alertas["Cantidad_tiendas"].apply(lambda x: "⚠️ Alerta" if x >= 3 else "Aviso" if x == 2 else None)
    df_alertas = df_alertas.dropna(subset=["Tipo"])
    # asegurar columnas esperadas
    for c in ["descripcion", "razon_social"]:
        if c not in df_alertas.columns:
            df_alertas[c] = None
    return df_alertas

# -------------------------
# helper: construir gráficos (seguro)
# -------------------------
def safe_bar_counts(series, label_x, label_y, title):
    if series is None or series.dropna().empty:
        return "<p style='color:#666;'>Sin datos</p>"
    dfp = series.value_counts().head(10).reset_index()
    dfp.columns = [label_x, label_y]
    fig = px.bar(dfp, x=label_x, y=label_y, title=title)
    fig.update_layout(template="plotly_dark")  # tarjetas oscuras
    return fig.to_html(full_html=False)

# -------------------------
# RUTA PRINCIPAL (render inicial)
# -------------------------
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = cargar_datos_locales()
    if df.empty:
        return templates.TemplateResponse("dashboard.html", {"request": request, "error": "Error al cargar datos."})

    # filtros: valores posibles para poblar selects (únicos)
    meses = sorted([int(x) for x in df["Mes"].dropna().unique() if pd.notna(x)]) if "Mes" in df.columns else []
    sub_tipos = sorted(df["sub_tipo_caso"].dropna().astype(str).unique().tolist()) if "sub_tipo_caso" in df.columns else []
    definiciones = sorted(df["definicion_calidad"].dropna().astype(str).unique().tolist()) if "definicion_calidad" in df.columns else []

    total_reclamos = len(df)
    df_alertas = generar_alertas(df)
    total_avisos = df_alertas[df_alertas["Tipo"] == "Aviso"].shape[0]
    total_alertas = df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"].shape[0]

    graf_proveedores = safe_bar_counts(df["razon_social"] if "razon_social" in df.columns else None,
                                      "Proveedor", "Cantidad", "Top 10 Proveedores con más reclamos")

    # mantenemos graf_productos solo si hay descripcion
    graf_productos = safe_bar_counts(df["descripcion"] if "descripcion" in df.columns else None,
                                     "Producto", "Cantidad", "Top 10 Productos más reclamados")

    # listados (top 20 para mostrar)
    df_alertas_sorted = df_alertas.sort_values("Cantidad_tiendas", ascending=False)
    listado_avisos = df_alertas_sorted[df_alertas_sorted["Tipo"] == "Aviso"].head(200).to_dict(orient="records")
    listado_alertas = df_alertas_sorted[df_alertas_sorted["Tipo"] == "⚠️ Alerta"].head(200).to_dict(orient="records")

    # proveedor top
    proveedor_top = df["razon_social"].mode()[0] if "razon_social" in df.columns and not df["razon_social"].dropna().empty else "-"

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "graf_proveedores": graf_proveedores,
        "graf_productos": graf_productos,
        "listado_avisos": listado_avisos,
        "listado_alertas": listado_alertas,
        "total_reclamos": total_reclamos,
        "total_avisos": total_avisos,
        "total_alertas": total_alertas,
        "proveedor_top": proveedor_top,
        "meses": meses,
        "sub_tipos": sub_tipos,
        "definiciones": definiciones,
        "error": None
    })

# -------------------------
# endpoint dinamico para filtros (POST con JSON)
# recibe: { mes: <int|null>, sub_tipo: <str|null>, definicion: <str|null> }
# devuelve JSON con htmls y listados
# -------------------------
@app.post("/filtrar")
async def filtrar_endpoint(payload: dict):
    try:
        mes = payload.get("mes", None)
        sub_tipo = payload.get("sub_tipo", None)
        definicion = payload.get("definicion", None)

        df = cargar_datos_locales()
        if df.empty:
            return JSONResponse({"error": "No se pudieron cargar datos."}, status_code=500)

        # aplicar filtros
        if mes:
            try:
                mes_int = int(mes)
                df = df[df["Mes"] == mes_int]
            except:
                pass
        if sub_tipo:
            df = df[df["sub_tipo_caso"].astype(str) == str(sub_tipo)]
        if definicion:
            df = df[df["definicion_calidad"].astype(str) == str(definicion)]

        total_reclamos = len(df)
        df_alertas = generar_alertas(df)
        total_avisos = df_alertas[df_alertas["Tipo"] == "Aviso"].shape[0]
        total_alertas = df_alertas[df_alertas["Tipo"] == "⚠️ Alerta"].shape[0]

        graf_proveedores = safe_bar_counts(df["razon_social"] if "razon_social" in df.columns else None,
                                          "Proveedor", "Cantidad", "Top 10 Proveedores con más reclamos")
        graf_productos = safe_bar_counts(df["descripcion"] if "descripcion" in df.columns else None,
                                         "Producto", "Cantidad", "Top 10 Productos más reclamados")

        df_alertas_sorted = df_alertas.sort_values("Cantidad_tiendas", ascending=False)
        listado_avisos = df_alertas_sorted[df_alertas_sorted["Tipo"] == "Aviso"].head(200)[["ean", "lote_nro", "Cantidad_tiendas", "descripcion", "razon_social"]].to_dict(orient="records")
        listado_alertas = df_alertas_sorted[df_alertas_sorted["Tipo"] == "⚠️ Alerta"].head(200)[["ean", "lote_nro", "Cantidad_tiendas", "descripcion", "razon_social"]].to_dict(orient="records")

        proveedor_top = df["razon_social"].mode()[0] if "razon_social" in df.columns and not df["razon_social"].dropna().empty else "-"

        return {
            "graf_proveedores": graf_proveedores,
            "graf_productos": graf_productos,
            "listado_avisos": listado_avisos,
            "listado_alertas": listado_alertas,
            "total_reclamos": total_reclamos,
            "total_avisos": total_avisos,
            "total_alertas": total_alertas,
            "proveedor_top": proveedor_top
        }

    except Exception as e:
        print("Error en /filtrar:", e)
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
