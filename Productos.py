import pandas as pd
import re
import unicodedata
import csv

 
# (EXTRACCIÓN)

df_productos = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Productos'
)

 
# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_productos.columns = df_productos.columns.str.strip().str.lower()

# Eliminar espacios extra en strings
df_productos = df_productos.map(lambda x: x.strip() if isinstance(x, str) else x)

# Convertir tipos de datos
df_productos = df_productos.astype({
    "id_producto": "string",
    "nombre": "string",
    "tipo": "string",
    "variedad": "string",
    "estado_madurez": "string",
    "especie": "string",
    "calibre": "string",
    "codigo_embalaje": "string",
    "n_caja": "string",
    "categoria": "string",
    "tipo_bin": "string",
    "id_proveedor": "string"
})

# Eliminar registros nulos en columnas clave
columnas_clave = ["id_producto", "nombre", "tipo", "id_proveedor"]
df_productos = df_productos.dropna(subset=columnas_clave)

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = str(texto)
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()

columnas_texto = ["nombre", "tipo", "variedad", "estado_madurez", "especie", "categoria", "tipo_bin"]
for col in columnas_texto:
    if col in df_productos.columns:
        df_productos[col] = df_productos[col].apply(normalizar_texto)

# Eliminar duplicados por Id_Producto
df_productos.drop_duplicates(subset=["id_producto"], keep="first", inplace=True)

# Detectar registros con datos faltantes
faltantes = df_productos[df_productos.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Rellenar valores faltantes en campos no críticos
campos_no_criticos = ["variedad", "estado_madurez", "especie", "calibre", "codigo_embalaje", "n_caja", "categoria", "tipo_bin"]
for campo in campos_no_criticos:
    if campo in df_productos.columns:
        df_productos[campo].fillna("Desconocido", inplace=True)

# Validar unicidad de la clave primaria
assert df_productos["id_producto"].is_unique, "Error: existen Id_Producto duplicados."

# Renombrar columnas según modelo dimensional
df_productos.rename(columns={
    "id_producto": "Id_Producto",
    "nombre": "Nombre",
    "tipo": "Tipo",
    "variedad": "Variedad",
    "estado_madurez": "Estado_Madurez",
    "especie": "Especie",
    "calibre": "Calibre",
    "codigo_embalaje": "Codigo_Embalaje",
    "n_caja": "N_Caja",
    "categoria": "Categoria",
    "tipo_bin": "Tipo_Bin",
    "id_proveedor": "Id_Proveedor"
}, inplace=True)

print("\n Tabla Dim_Productos limpia y transformada:")
print(df_productos.head())

 
# (CARGA)

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\productostransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    escritor.writerow(df_productos.columns)
    escritor.writerows(df_productos.values.tolist())

print("\n✅ Proceso ETL completado con éxito.")
print(f"📁 Archivo CSV guardado en: {ruta_salida}")