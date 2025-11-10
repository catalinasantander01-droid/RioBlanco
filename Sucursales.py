import pandas as pd
import re
import unicodedata
import csv
 
# (EXTRACCIÓN)

df_sucursales = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Sucursales'
)
 
# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_sucursales.columns = df_sucursales.columns.str.strip().str.lower()

# Eliminar espacios extras en strings
df_sucursales = df_sucursales.map(lambda x: x.strip() if isinstance(x, str) else x)

# Convertir tipos de datos
df_sucursales = df_sucursales.astype({
    "id_sucursal": "string",
    "nombre_cliente": "string",
    "región": "string",
    "país": "string",
    "continente": "string"
})

# Eliminar registros nulos en columnas clave
columnas_clave = ["id_sucursal", "nombre_cliente", "región"]
df_sucursales = df_sucursales.dropna(subset=columnas_clave)

# Eliminar duplicados por Id_Sucursal
df_sucursales = df_sucursales.drop_duplicates(subset=["id_sucursal"], keep="first")

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    texto = texto.replace(',', '').replace('.', '')
    return texto.title()

df_sucursales["nombre_cliente"] = df_sucursales["nombre_cliente"].apply(normalizar_texto)
df_sucursales["región"] = df_sucursales["región"].apply(normalizar_texto)
df_sucursales["país"] = df_sucursales["país"].apply(normalizar_texto)
df_sucursales["continente"] = df_sucursales["continente"].apply(normalizar_texto)

# Detectar registros faltantes
faltantes = df_sucursales[df_sucursales.isnull().any(axis=1)]
if not faltantes.empty:
    print("Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Rellenar valores faltantes en campos no críticos
df_sucursales.fillna({
    "región": "Desconocida",
    "país": "Desconocido",
    "continente": "Desconocido"
}, inplace=True)

# Asegurar unicidad de la clave primaria
assert df_sucursales["id_sucursal"].is_unique, "Error: existen Id_Sucursal duplicados."

# Renombrar columnas según modelo Dimensional
df_sucursales.rename(columns={
    "id_sucursal": "Id_Sucursal",
    "nombre_cliente": "Nombre_Cliente",
    "región": "Region",
    "país": "Pais",
    "continente": "Continente"
}, inplace=True)

print("\n Tabla Dim_Sucursales limpia y transformada:")
print(df_sucursales.head())

 
# (CARGA)

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\sucursalestransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    
    # Escribir encabezados
    escritor.writerow(df_sucursales.columns)
    
    # Escribir filas del DataFrame
    escritor.writerows(df_sucursales.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")