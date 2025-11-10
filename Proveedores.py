import pandas as pd
import numpy as np
import re
import unicodedata

 
# (EXTRACCIÓN)

df_proveedores = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Proveedores'
)

 
# (TRANSFORMACIÓN)

# Estandarizar nombres 
df_proveedores.columns = df_proveedores.columns.str.strip().str.lower()

# Asegurar codificación y tipos correctos
df_proveedores = df_proveedores.map(lambda x: x.strip() if isinstance(x, str) else x)  # eliminar espacios extras
df_proveedores = df_proveedores.astype({
    "id_proveedores": "string",
    "nombre": "string",
    "telefono": "string",
    "correo": "string",
    "ciudad": "string"
})

# Eliminar registros nulos o incompletos en columnas clave
columnas_clave = ["id_proveedores", "nombre", "telefono"]
df_proveedores = df_proveedores.dropna(subset=columnas_clave)

# Eliminar duplicados basados en Id_Proveedor o Nombre
if "id_proveedor" in df_proveedores.columns:
    df_proveedores.drop_duplicates(subset=["id_proveedor"], keep="first", inplace=True)
else:
    df_proveedores.drop_duplicates(subset=["nombre"], keep="first", inplace=True)

# Normalizar texto 
def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()

df_proveedores["nombre"] = df_proveedores["nombre"].apply(normalizar_texto)
df_proveedores["ciudad"] = df_proveedores["ciudad"].apply(normalizar_texto)

# Limpiar y validar teléfonos
def limpiar_telefono(telefono):
    if pd.isnull(telefono):
        return np.nan
    telefono = re.sub(r"[^\d+]", "", telefono)
    if 7 <= len(telefono) <= 12:
        return telefono
    else:
        return np.nan

df_proveedores["telefono"] = df_proveedores["telefono"].apply(limpiar_telefono)

# Corregir errores comunes de ciudad
ciudades_validas = {
    "coquimbo": "Coquimbo", "valparaiso": "Valparaíso", "la serena": "La Serena",
    "santiago": "Santiago", "concepcion": "Concepción", "temuco": "Temuco",
    "antofagasta": "Antofagasta", "iquique": "Iquique", "arica": "Arica",
    "puerto montt": "Puerto Montt", "osorno": "Osorno", "chillan": "Chillán",
    "rancagua": "Rancagua", "ovalle": "Ovalle", "copiapo": "Copiapó"
}
df_proveedores["ciudad"] = df_proveedores["ciudad"].apply(
    lambda x: ciudades_validas.get(x.lower(), x.title()) if isinstance(x, str) else x
)

# Eliminar duplicados 
df_proveedores = df_proveedores.drop_duplicates(subset=["id_proveedores"], keep="first")

# Detectar registros faltantes tras limpieza
faltantes = df_proveedores[df_proveedores.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Rellenar valores faltantes en campos no críticos
df_proveedores.fillna({"ciudad": "Desconocida"}, inplace=True)

# Asegurar unicidad de la clave primaria
assert df_proveedores["id_proveedores"].is_unique, "Error: existen Id_Proveedores duplicados."

# Renombrar columnas según modelo Dimensional
df_proveedores.rename(columns={
    "id_proveedores": "Id_Proveedor",
    "nombre": "Nombre",
    "telefono": "Telefono",
    "correo": "Correo",
    "ciudad": "Ciudad"
}, inplace=True)


print("\n Tabla Dim_Proveedores limpia y transformada:")
print(df_proveedores.head())

#(CARGA)

import csv

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\proveedorestransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    
    # Escribir encabezados
    escritor.writerow(df_proveedores.columns)
    
    # Escribir filas del DataFrame
    escritor.writerows(df_proveedores.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")

