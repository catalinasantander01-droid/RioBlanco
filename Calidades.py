import pandas as pd
import csv

# (EXTRACCIÓN)
df_calidades = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Calidades'
)

# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_calidades.columns = df_calidades.columns.str.strip().str.lower()

# Corregir nombres con espacios o errores
df_calidades.rename(columns={
    "bins_ vaciados": "bins_vaciados"
}, inplace=True)

# Eliminar espacios extra en strings 
df_calidades = df_calidades.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Reemplazar NaN o valores no numéricos por 0 antes de convertir
for col in ["bins_recepcionados", "bins_vaciados", "cajas_calidad"]:
    if col in df_calidades.columns:
        df_calidades[col] = pd.to_numeric(df_calidades[col], errors='coerce').fillna(0)

# Convertir tipos de datos 
dtype_map = {}
if "sag_codigo" in df_calidades.columns:
    dtype_map["sag_codigo"] = "string"
if "sag_codigo_packing" in df_calidades.columns:
    dtype_map["sag_codigo_packing"] = "string"
for col in ["bins_recepcionados", "bins_vaciados", "cajas_calidad"]:
    if col in df_calidades.columns:
        dtype_map[col] = "int64"
if dtype_map:
    df_calidades = df_calidades.astype(dtype_map)

# Validar coherencia entre bins recepcionados y vaciados 
if {"bins_recepcionados", "bins_vaciados"}.issubset(df_calidades.columns):
    df_calidades["coherente"] = df_calidades["bins_recepcionados"] == df_calidades["bins_vaciados"]
    inconsistentes = df_calidades[~df_calidades["coherente"]]
    if not inconsistentes.empty:
        print("Registros con incoherencia entre bins recepcionados y vaciados:")
        print(inconsistentes[["sag_codigo", "sag_codigo_packing", "bins_recepcionados", "bins_vaciados"]])
    df_calidades.drop(columns=["coherente"], inplace=True)

# Eliminar duplicados por SAG_Codigo + SAG_Codigo_packing
if {"sag_codigo", "sag_codigo_packing"}.issubset(df_calidades.columns):
    df_calidades.drop_duplicates(subset=["sag_codigo", "sag_codigo_packing"], keep="first", inplace=True)

# Detectar registros con datos faltantes
faltantes = df_calidades[df_calidades.isnull().any(axis=1)]
if not faltantes.empty:
    print("Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Renombrar columnas
rename_map = {
    "sag_codigo": "SAG_Codigo",
    "sag_codigo_packing": "SAG_Codigo_Packing",
    "bins_recepcionados": "Bins_Recepcionados",
    "bins_vaciados": "Bins_Vaciados",
    "cajas_calidad": "Cajas_calidad"
}
df_calidades.rename(columns={k: v for k, v in rename_map.items() if k in df_calidades.columns}, inplace=True)

print("\n Tabla Hecho_Calidades limpia y transformada:")
print(df_calidades.head())

def eliminar_sin_sag(df, col_sag="SAG_Codigo", col_packing="SAG_Codigo_Packing", mostrar_ejemplo=True):
    df_local = df.copy()
    for c in [col_sag, col_packing]:
        if c in df_local.columns:
            df_local[c] = df_local[c].astype("string").str.strip()
        else:
            df_local[c] = pd.Series([pd.NA] * len(df_local), index=df_local.index, dtype="string")

    # Conservar filas donde al menos una de las columnas tenga valor no nulo y no vacío
    tiene_sag = df_local[col_sag].notna() & (df_local[col_sag] != "")
    tiene_packing = df_local[col_packing].notna() & (df_local[col_packing] != "")
    conservar = tiene_sag | tiene_packing

    total_inicial = len(df_local)
    total_conservadas = conservar.sum()
    total_eliminadas = total_inicial - int(total_conservadas)

    # Mostrar ejemplo de filas eliminadas
    if mostrar_ejemplo and total_eliminadas > 0:
        print(f"\nSe eliminarán {total_eliminadas} filas que no tienen ni {col_sag} ni {col_packing}. Ejemplo:")
        print(df_local.loc[~conservar].head(10))

    # Retornar DataFrame filtrado 
    df_filtrado = df_local.loc[conservar].copy()
    return df_filtrado

df_calidades = eliminar_sin_sag(df_calidades, mostrar_ejemplo=True)

# CARGA
ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\calidadestransformados.csv"
df_calidades.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("\n Proceso ETL completado con éxito.")
print(f"Archivo CSV guardado en: {ruta_salida}")