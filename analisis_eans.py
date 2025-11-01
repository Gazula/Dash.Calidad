import pandas as pd
import matplotlib.pyplot as plt

# ============================
# 1. CARGA DE ARCHIVOS
# ============================

# Archivo principal con reclamos
informe_path = "Reclamos Ene-Sep 2025.xlsx"
# Base de datos de productos
base_path = "Base de datos.xlsx"

print("Leyendo archivos...")
df_informe = pd.read_excel(informe_path)
df_base = pd.read_excel(base_path)

# Normalizamos nombres de columnas (por si acaso)
df_informe.columns = df_informe.columns.str.strip().str.lower()
df_base.columns = df_base.columns.str.strip().str.lower()

# ============================
# 2. UNIFICACIÓN POR EAN
# ============================

print("Combinando información por EAN...")

df_final = df_informe.merge(
    df_base[['ean', 'descripcion', 'razon_social']],
    on='ean',
    how='left'
)

# Reemplazar los valores faltantes con "No tipificado"
df_final['descripcion'] = df_final['descripcion'].fillna('No tipificado')
df_final['razon_social'] = df_final['razon_social'].fillna('No tipificado')

# ============================
# 3. ESTADÍSTICAS
# ============================

print("Generando estadísticas...")

# Conteo de productos y proveedores
conteo_productos = df_final['descripcion'].value_counts()
conteo_proveedores = df_final['razon_social'].value_counts()

# ============================
# 4. GRÁFICOS AUTOMÁTICOS
# ============================

print("Creando gráficos...")

# Top 10 productos más frecuentes
plt.figure(figsize=(10,5))
conteo_productos.head(10).plot(kind='bar', title='Top 10 productos más frecuentes')
plt.xlabel('Producto')
plt.ylabel('Cantidad de apariciones')
plt.tight_layout()
plt.savefig('top_productos.png', dpi=300)
plt.close()

# Top 10 proveedores más frecuentes
plt.figure(figsize=(10,5))
conteo_proveedores.head(10).plot(kind='bar', color='orange', title='Top 10 proveedores más frecuentes')
plt.xlabel('Proveedor')
plt.ylabel('Cantidad de apariciones')
plt.tight_layout()
plt.savefig('top_proveedores.png', dpi=300)
plt.close()

# ============================
# 5. EXPORTAR INFORME FINAL
# ============================

salida = "Informe EANs Tipificados.xlsx"
df_final.to_excel(salida, index=False)

print(f"\n✅ Proceso completado con éxito.")
print(f"📁 Archivo generado: {salida}")
print(f"📊 Gráficos: top_productos.png y top_proveedores.png")