import pandas as pd
import re
import unicodedata
import csv

 
# (EXTRACCIÓN)

df_cambios = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Cambios'
)

 
# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_cambios.columns = df_cambios.columns.str.strip().str.lower()

# Eliminar espacios extras en strings
df_cambios = df_cambios.map(lambda x: x.strip() if isinstance(x, str) else x)

# Convertir tipos de datos
df_cambios = df_cambios.astype({
    "id_cambio": "string",
    "nombre_error": "string",
    "especie_error": "string",
    "id_categoria": "string"
})

# Eliminar registros nulos en columnas clave
columnas_clave = ["id_cambio", "nombre_error", "especie_error"]
df_cambios = df_cambios.dropna(subset=columnas_clave)

# Eliminar duplicados por Id_Cambio
df_cambios = df_cambios.drop_duplicates(subset=["id_cambio"], keep="first")

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()

df_cambios["nombre_error"] = df_cambios["nombre_error"].apply(normalizar_texto)
df_cambios["especie_error"] = df_cambios["especie_error"].apply(normalizar_texto)

# Detectar registros faltantes
faltantes = df_cambios[df_cambios.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Rellenar valores faltantes en campos no críticos
df_cambios.fillna({"id_categoria": "Sin categoría"}, inplace=True)

# Asegurar unicidad de la clave primaria
assert df_cambios["id_cambio"].is_unique, "Error: existen Id_Cambio duplicados."

# Renombrar columnas 
df_cambios.rename(columns={
    "id_cambio": "Id_Cambio",
    "nombre_error": "Nombre_Error",
    "especie_error": "Especie_Error",
    "id_categoria": "Id_Categoria"
}, inplace=True)

print("\n Tabla Dim_Cambios limpia y transformada:")
print(df_cambios.head())

 
# (CARGA)

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\cambiostransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    
    # Escribir encabezados
    escritor.writerow(df_cambios.columns)
    
    # Escribir filas del DataFrame
    escritor.writerows(df_cambios.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")