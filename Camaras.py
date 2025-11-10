import pandas as pd
import re
import unicodedata

# (EXTRACCIÓN)

df_camaras = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Camaras'
)

 
# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas 
df_camaras.columns = df_camaras.columns.str.strip().str.lower()

# Asegurar codificación y tipos correctos
df_camaras = df_camaras.map(lambda x: x.strip() if isinstance(x, str) else x)  # eliminar espacios extras
df_camaras = df_camaras.astype({
    "id_camara": "string",
    "nombre_camara": "string"
})

# Eliminar registros nulos o incompletos en columnas clave
columnas_clave = ["id_camara", "nombre_camara", "capacidad"]
df_camaras = df_camaras.dropna(subset=columnas_clave)

# Eliminar duplicados basados en Id_Proveedor o Nombre
if "id_camara" in df_camaras.columns:
    df_camaras.drop_duplicates(subset=["id_camara"], keep="first", inplace=True)
else:
    df_camaras.drop_duplicates(subset=["nombre_camara"], keep="first", inplace=True)

def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = str(texto)  # convierte todo a texto
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()


df_camaras["nombre_camara"] = df_camaras["nombre_camara"].apply(normalizar_texto)


# Eliminar duplicados 
df_camaras = df_camaras.drop_duplicates(subset=["id_camara"], keep="first")

# Detectar registros faltantes tras limpieza
faltantes = df_camaras[df_camaras.isnull().any(axis=1)]
if not faltantes.empty:
    print("Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Rellenar valores faltantes en campos no críticos
df_camaras.fillna({"capacidad": "Desconocida"}, inplace=True)

# Asegurar unicidad de la clave primaria
assert df_camaras["id_camara"].is_unique, "Error: existen Id_Proveedores duplicados."

# Renombrar columnas 
df_camaras.rename(columns={
    "id_camara": "Id_Camara",
    "nombre_camara": "Nombre_Camara",
    "capacidad": "Capacidad"
}, inplace=True)


print("\n Tabla Dim_Camaras limpia y transformada:")
print(df_camaras.head())


#(CARGA)


import csv

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\camarastransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    
    # Escribir encabezados
    escritor.writerow(df_camaras.columns)
    
    # Escribir filas del DataFrame
    escritor.writerows(df_camaras.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")


