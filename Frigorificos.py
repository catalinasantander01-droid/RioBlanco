import pandas as pd
import csv
from datetime import datetime
import re

 
# (EXTRACCIÓN)

df_frigorificos = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Frigorificos'
)

# Mostrar columnas originales para depuración
print("Columnas originales:", df_frigorificos.columns.tolist())

# Eliminar columnas duplicadas
columnas_a_quitar = [col for col in df_frigorificos.columns if ".1" in col]
df_frigorificos.drop(columns=columnas_a_quitar, inplace=True)

# Estandarizar nombres de columnas
df_frigorificos.columns = df_frigorificos.columns.str.strip().str.lower()

# Eliminar espacios extra
df_frigorificos = df_frigorificos.map(lambda x: x.strip() if isinstance(x, str) else x)

# Convertir tipos
df_frigorificos = df_frigorificos.astype({
    "id_frigorifico": "string",
    "id_camara": "string"
})

# Formatear fechas
def formatear_fecha(valor):
    if pd.isnull(valor): return ""
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


def formatear_hora(valor):
    if pd.isnull(valor): return ""
    if isinstance(valor, (datetime, pd.Timestamp)):
        return valor.strftime("%H:%M:%S")
    texto = str(valor).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(texto, fmt).strftime("%H:%M:%S")
        except ValueError:
            continue
    return texto


df_frigorificos["fecha_entrada"] = df_frigorificos["fecha_entrada"].apply(formatear_fecha)
df_frigorificos["fecha_salida"] = df_frigorificos["fecha_salida"].apply(formatear_fecha)
df_frigorificos["hora_entrada"] = df_frigorificos["hora_entrada"].apply(formatear_hora)
df_frigorificos["hora_salida"] = df_frigorificos["hora_salida"].apply(formatear_hora)

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto): return texto
    texto = str(texto).strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.upper()

df_frigorificos["id_camara"] = df_frigorificos["id_camara"].apply(normalizar_texto)

# Eliminar duplicados
df_frigorificos.drop_duplicates(subset=["id_frigorifico"], keep="first", inplace=True)

# Detectar faltantes
faltantes = df_frigorificos[df_frigorificos.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Renombrar columnas 
df_frigorificos.rename(columns={
    "id_frigorifico": "Id_Frigorifico",
    "fecha_entrada": "Fecha_Entrada",
    "fecha_salida": "Fecha_Salida",
    "hora_entrada": "Hora_Entrada",
    "hora_salida": "Hora_Salida",
    "id_camara": "Id_Camara"
}, inplace=True)

print("\n Tabla Dim_Frigorificos limpia y transformada:")
print(df_frigorificos.head())

 
# (CARGA)

ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\frigorificostransformados.csv"

with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    escritor.writerow(df_frigorificos.columns)
    escritor.writerows(df_frigorificos.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")