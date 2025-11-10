import pandas as pd
import re
import unicodedata
from datetime import datetime, time
import csv

# (EXTRACCIÓN)
ruta_excel = r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx'
df_packings = pd.read_excel(ruta_excel, sheet_name='Packings')

# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_packings.columns = df_packings.columns.str.strip().str.lower()

# Limpiar espacios en columnas de texto
for col in df_packings.columns:
    if pd.api.types.is_string_dtype(df_packings[col]) or pd.api.types.is_object_dtype(df_packings[col]):
        df_packings[col] = df_packings[col].astype("string").str.strip()

# Función para formatear fechas (días)
def formatear_fecha(valor):
    if pd.isnull(valor) or str(valor).strip() == "":
        return ""
    if isinstance(valor, (datetime, pd.Timestamp)):
        return valor.strftime("%Y-%m-%d")
    texto = str(valor).strip().replace(".", "-").replace("/", "-")
    formatos = ["%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%Y-%d-%m", "%m-%d-%Y", "%m-%d-%y", "%d-%m"]
    for fmt in formatos:
        try:
            fecha = datetime.strptime(texto, fmt)
            if fecha.year < 100:
                fecha = fecha.replace(year=2000 + fecha.year)
            return fecha.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""

# Función para formatear horas
def formatear_hora(valor):
    if pd.isnull(valor) or str(valor).strip() == "":
        return ""
    if isinstance(valor, (datetime, pd.Timestamp)):
        return valor.strftime("%H:%M:%S")
    if isinstance(valor, time):
        return valor.strftime("%H:%M:%S")
    texto = str(valor).strip()
    # Normalizar separadores y eliminar texto irrelevante 
    texto = texto.replace(".", ":").replace(",", ":")
    texto = re.sub(r'\s+', ' ', texto)
    # Intentos de parseo con formatos comunes
    formatos = ["%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M:%S %p"]
    # si viene como número tipo 900 -> interpretamos como HHMM
    if re.fullmatch(r'\d{3,4}', texto):
        if len(texto) == 3:
            texto = "0" + texto
        texto = texto[:2] + ":" + texto[2:]
    # quitar todo menos dígitos, dos puntos y AM/PM
    m = re.match(r'^\s*([0-9:]{1,8})(\s*[APMapm\.]{0,4})\s*$', texto)
    if m:
        candidato = (m.group(1) + " " + (m.group(2) or "")).strip()
    else:
        candidato = texto
    for fmt in formatos:
        try:
            hora = datetime.strptime(candidato, fmt)
            return hora.strftime("%H:%M:%S")
        except ValueError:
            continue
    return ""

# Aplicar transformaciones
df_packings["fecha_salida"] = df_packings.get("fecha_salida").apply(formatear_fecha)
if "hora_despacho" in df_packings.columns:
    df_packings["hora_despacho"] = df_packings["hora_despacho"].apply(formatear_hora)
else:
    df_packings["hora_despacho"] = ""

# Convertir tipos de datos
df_packings = df_packings.astype({
    "id_packing": "string",
    "linea_proceso": "string",
    "tipo_salida": "string",
    "fecha_salida": "string",
    "hora_despacho": "string",
    "id_sucursal": "string"
})

# Eliminar registros nulos en columnas clave
columnas_clave = ["id_packing", "linea_proceso", "tipo_salida", "fecha_salida"]
df_packings = df_packings.dropna(subset=columnas_clave)

# Eliminar duplicados por Id_Packing
df_packings = df_packings.drop_duplicates(subset=["id_packing"], keep="first")

# Normalizar texto
def normalizar_texto(texto):
    if pd.isnull(texto) or texto == "":
        return texto
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = texto.strip().lower()
    texto = re.sub(r'\s+', ' ', texto)
    return texto.title()

df_packings["tipo_salida"] = df_packings["tipo_salida"].apply(normalizar_texto)

# Rellenar valores faltantes en campos no críticos
df_packings["id_sucursal"].fillna("Sin sucursal", inplace=True)

# Asegurar unicidad de la clave primaria
assert df_packings["id_packing"].is_unique, "Error: existen Id_Packing duplicados."

# Renombrar columnas 
df_packings.rename(columns={
    "id_packing": "Id_Packing",
    "linea_proceso": "Linea_Proceso",
    "tipo_salida": "Tipo_Salida",
    "fecha_salida": "Fecha_Salida",
    "hora_despacho": "Hora_Despacho",
    "id_sucursal": "Id_Sucursal"
}, inplace=True)

# (CARGA)
ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\packingstransformados.csv"
df_packings.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")