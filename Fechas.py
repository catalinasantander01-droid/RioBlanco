import pandas as pd
import csv
from datetime import datetime

 
# (EXTRACCIÓN)

df_fechas = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Fechas'
)

 
# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_fechas.columns = df_fechas.columns.str.strip().str.lower()

# Eliminar espacios extra en strings
df_fechas = df_fechas.map(lambda x: x.strip() if isinstance(x, str) else x)

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
    return ""


df_fechas["dia"] = df_fechas["dia"].apply(formatear_fecha)

# Convertir tipos de datos de columnas no numéricas a string
df_fechas = df_fechas.astype({
    "id_fecha": "string",
    "dia": "string",
    "hora": "string",
    "mes": "string",
    "semestre": "string",
    "año": "string",
    "temporada": "string",
    "trimestre": "string"
})

# Eliminar duplicados por Id_Fecha
df_fechas.drop_duplicates(subset=["id_fecha"], keep="first", inplace=True)

# Detectar registros con datos faltantes
faltantes = df_fechas[df_fechas.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Validar unicidad de la clave primaria
assert df_fechas["id_fecha"].is_unique, "Error: existen Id_Fecha duplicados."

# Renombrar columnas según modelo dimensional
df_fechas.rename(columns={
    "id_fecha": "Id_Fecha",
    "dia": "Dia",
    "hora": "Hora",
    "mes": "Mes",
    "semestre": "Semestre",
    "año": "Año",
    "temporada": "Temporada",
    "trimestre": "Trimestre"
}, inplace=True)

print("\n Tabla Dim_Fechas limpia y transformada:")
print(df_fechas.head())

 
# (CARGA)

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\fechastransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    escritor.writerow(df_fechas.columns)
    escritor.writerows(df_fechas.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")
