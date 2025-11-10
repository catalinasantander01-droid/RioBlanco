import pandas as pd
import csv
from datetime import datetime
import re

# EXTRACCIÓN

df_lotes = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Lotes'
)

# TRANSFORMACIÓN

# Estandarizar nombres de columnas
df_lotes.columns = df_lotes.columns.str.strip().str.lower()

# Eliminar espacios extra en strings
df_lotes = df_lotes.map(lambda x: x.strip() if isinstance(x, str) else x)

# Convertir tipos de datos
df_lotes = df_lotes.astype({
    "id_lote": "string",
    "transporte": "string",
    "origen": "string"
})

# Convertir N_Cuartel y N_Lote a enteros
df_lotes["n_cuartel"] = pd.to_numeric(df_lotes["n_cuartel"], errors="coerce", downcast="integer")
df_lotes["n_lote"] = pd.to_numeric(df_lotes["n_lote"], errors="coerce", downcast="integer")

# Asegurar que las fechas tengan formato consistente (YYYY-MM-DD)
def formatear_fecha(valor):
    if pd.isnull(valor):
        return ""
    if isinstance(valor, (datetime, pd.Timestamp)):
        return valor.strftime("%Y-%m-%d")
    
    texto = str(valor).strip()
    texto = texto.replace(".", "-").replace("/", "-")
    
    formatos = [
        "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%Y-%d-%m",
        "%m-%d-%Y", "%m-%d-%y", "%d-%m"
    ]
    
    for fmt in formatos:
        try:
            fecha = datetime.strptime(texto, fmt)
            if fecha.year < 100:
                fecha = fecha.replace(year=2000 + fecha.year)
            return fecha.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return texto

# Aplicar transformación
df_lotes["fecha_cosecha"] = df_lotes["fecha_cosecha"].apply(formatear_fecha)

# Asegurarse que la columna es string
df_lotes["fecha_cosecha"] = df_lotes["fecha_cosecha"].astype(str)

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto):
        return texto
    texto = str(texto)
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()

df_lotes["transporte"] = df_lotes["transporte"].apply(normalizar_texto)
df_lotes["origen"] = df_lotes["origen"].apply(normalizar_texto)

# Eliminar duplicados por Id_Lote
df_lotes.drop_duplicates(subset=["id_lote"], keep="first", inplace=True)

# Detectar registros con datos faltantes
faltantes = df_lotes[df_lotes.isnull().any(axis=1)]
if not faltantes.empty:
    print("Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Renombrar columnas
df_lotes.rename(columns={
    "id_lote": "Id_Lote",
    "n_cuartel": "N_Cuartel",
    "n_lote": "N_Lote",
    "transporte": "Transporte",
    "origen": "Origen",
    "fecha_cosecha": "Fecha_Cosecha"
}, inplace=True)

print("\nTabla Dim_Lotes limpia y transformada:")
print(df_lotes.head())

# CARGA

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\lotestransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    escritor.writerow(df_lotes.columns)
    escritor.writerows(df_lotes.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")