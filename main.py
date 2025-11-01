from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import plotly.express as px
import io
import requests

# ====================================
# 🔧 CONFIGURACIÓN INICIAL DEL SERVIDOR
# ====================================

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ====================================
# 📂 FUNCIONES AUXILIARES
# ====================================

def descargar_excel_desde_drive(file_id: str) -> pd.DataFrame:
    """Descarga un archivo Excel desde Google Drive y lo devuelve como DataFrame."""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_excel(io.BytesIO(response.content))
        print(f"✅ Archivo descargado correctamente desde Drive ({file_id})")
        return df
    except Exception as e:
        print(f"⚠️ Error descargando archivo desde Drive ({file_id}): {e}")
        return pd.DataFrame()


def procesar_datos():
    """Descarga los datos desde Drive o desde archivos locales, y realiza la unión por EAN."""
    # 🔹 IDs de Drive (reemplazá por los tuyos si querés usar Drive)
    ID_BASE_DATOS = "TU_ID_BASE_DATOS"
    ID_RECLAMOS = "TU_ID_RECLAMOS"

    # 🔹 Intentar descargar desde Drive
    df_base = descargar_excel_desde_drive(ID_BASE_DATOS)
    df_reclamos = descargar_excel_desde_drive(ID_RECLAMOS)

    # 🔹 Si están vacíos, intentar usar archivos locales
    if df_base.empty or df_reclamos.empty:
        try:
            df_base = pd.read_excel("Base de datos.xlsx")
            df_reclamos = pd.read_excel("Reclamos Ene-Sep 2025.xlsx")
            print("📂 Archivos locales cargados correctamente.")
        except Exception as e:
            print(f"⚠️ Error cargando archivos locales: {e}")
            return pd.DataFrame()

    # 🔹 Unir por EAN
    df = pd.merge(df_reclamos, df_base[["EAN", "Descripción", "Razón social"]],
                  on="EAN", how="left")

    # 🔹 Rellenar vacíos
    df["Descripción"] = df["Descripción"].fillna("No tipificado")
    df["Razón social"] = df["Razón social"].fillna("No tipificado")

    # 🔹 Separar fecha y hora
    if "Fecha/hora de apertura" in df.columns:
        df["Fecha apertura"] = pd.to_datetime(df["Fecha/hora de apertura"], errors="coerce").dt.date
        df["Hora apertura"] = pd.to_datetime(df["Fecha/hora de apertura"], errors="coerce").dt.time

    return df


def detectar_alertas(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta reclamos repetidos con mismo EAN y Lote en distintas tiendas."""
    try:
        if "EAN" not in df.columns or "Lote nro." not in df.columns:
            return pd.DataFrame()

        agrupado = df.groupby(["EAN", "Lote nro."])["Código de sucursal"].nunique().reset_index()
        agrupado.columns = ["EAN", "Lote nro.", "Cantidad_tiendas"]
        agrupado = agrupado[agrupado["Cantidad_tiendas"] > 1]

        def tipo_alerta(x):
            if x >= 3:
                return "🚨 Alerta"
            elif x == 2:
                return "⚠️ Aviso"
            else:
                return "-"

        agrupado["Tipo"] = agrupado["Cantidad_tiendas"].apply(tipo_alerta)
        return agrupado
    except Exception as e:
        print(f"⚠️ Error detectando alertas: {e}")
        return pd.DataFrame()


# ====================================
# 🧭 ENDPOINT PRINCIPAL (DASHBOARD)
# ====================================

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

    # ==========================
    # 🔹 Gráfico 1 — Proveedores
    # ==========================
    try:
        top_proveedores = df["Razón social"].value_counts().reset_index()
        top_proveedores.columns = ["Razón social", "count"]

        graf_proveedores = px.bar(
            top_proveedores.head(10),
            x="Razón social",
            y="count",
            labels={"Razón social": "Proveedor", "count": "Cantidad de Reclamos"},
            title="Top 10 Proveedores con más Reclamos",
        )
        graf_proveedores.update_layout(
            xaxis_tickangle=-45,
            title_x=0.5,
            margin=dict(l=40, r=40, t=60, b=100),
            plot_bgcolor="rgba(0,0,0,0)",
        )
        graf_proveedores = graf_proveedores.to_html(full_html=False)

    except Exception as e:
        print(f"⚠️ Error generando gráfico de proveedores: {e}")
        graf_proveedores = "<p>Error al generar gráfico de proveedores.</p>"

    # ==========================
    # 🔹 Gráfico 2 — Productos
    # ==========================
    try:
        top_productos = df["Descripción"].value_counts().reset_index()
        top_productos.columns = ["Descripción", "count"]

        graf_productos = px.bar(
            top_productos.head(10),
            x="Descripción",
            y="count",
            labels={"Descripción": "Producto", "count": "Cantidad de Reclamos"},
            title="Top 10 Productos más Reclamados",
        )
        graf_productos.update_layout(
            xaxis_tickangle=-45,
            title_x=0.5,
            margin=dict(l=40, r=40, t=60, b=100),
            plot_bgcolor="rgba(0,0,0,0)",
        )
        graf_productos = graf_productos.to_html(full_html=False)

    except Exception as e:
        print(f"⚠️ Error generando gráfico de productos: {e}")
        graf_productos = "<p>Error al generar gráfico de productos.</p>"

    # ==========================
    # 🔹 Gráfico 3 — Alertas
    # ==========================
    try:
        if not df_alertas.empty:
            graf_alertas = px.bar(
                df_alertas,
                x="EAN",
                y="Cantidad_tiendas",
                color="Tipo",
                labels={
                    "EAN": "Código EAN",
                    "Cantidad_tiendas": "Cantidad de Tiendas",
                    "Tipo": "Tipo de Alerta",
                },
                title="Alertas detectadas (EAN + Lote con reclamos en múltiples tiendas)",
            )
            graf_alertas.update_layout(
                title_x=0.5,
                margin=dict(l=40, r=40, t=60, b=100),
                plot_bgcolor="rgba(0,0,0,0)",
            )
            graf_alertas = graf_alertas.to_html(full_html=False)
        else:
            graf_alertas = "<p>No se detectaron alertas.</p>"

    except Exception as e:
        print(f"⚠️ Error generando gráfico de alertas: {e}")
        graf_alertas = "<p>Error al generar gráfico de alertas.</p>"

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


# ====================================
# 🚀 EJECUCIÓN LOCAL
# ====================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
