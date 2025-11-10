import pandas as pd
import re
import unicodedata
import csv

 
# (EXTRACCIÓN)

df_categorias = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Categorias'
)

 
# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_categorias.columns = df_categorias.columns.str.strip().str.lower()

# Eliminar espacios extras en strings
df_categorias = df_categorias.map(lambda x: x.strip() if isinstance(x, str) else x)

# Convertir tipos de datos
df_categorias = df_categorias.astype({
    "id_categoria": "string",
    "nombre": "string",
    "especie": "string"
})

# Eliminar registros nulos en columnas clave
columnas_clave = ["id_categoria", "nombre", "especie"]
df_categorias = df_categorias.dropna(subset=columnas_clave)

# Eliminar duplicados por Id_Categoria
df_categorias = df_categorias.drop_duplicates(subset=["id_categoria"], keep="first")

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()

df_categorias["nombre"] = df_categorias["nombre"].apply(normalizar_texto)
df_categorias["especie"] = df_categorias["especie"].apply(normalizar_texto)

# Detectar registros faltantes
faltantes = df_categorias[df_categorias.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Rellenar valores faltantes en campos no críticos
df_categorias.fillna({"especie": "Sin especie"}, inplace=True)

# Asegurar que la clave primaria sea única
assert df_categorias["id_categoria"].is_unique, "Error: existen Id_Categoria duplicados."

# Renombrar columnas 
df_categorias.rename(columns={
    "id_categoria": "Id_Categoria",
    "nombre": "Nombre",
    "especie": "Especie"
}, inplace=True)

print("\n Tabla Dim_Categorías limpia y transformada:")
print(df_categorias.head())

 
# (CARGA)

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\categoriastransformadas.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    
    # Escribir encabezados
    escritor.writerow(df_categorias.columns)
    
    # Escribir filas del DataFrame
    escritor.writerows(df_categorias.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")