import pandas as pd
import csv

# (EXTRACCIÓN)
df_procesos = pd.read_excel(
    r'C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\datos.xlsx',
    sheet_name='Procesos'
)

# (TRANSFORMACIÓN)

# Estandarizar nombres de columnas
df_procesos.columns = df_procesos.columns.str.strip().str.lower()

# Eliminar espacios extra en strings
df_procesos = df_procesos.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Reemplazar NaN por 0 en las columnas numéricas antes de convertir
for col in ["cantidad_recepcion", "cantidad_rechazada", "cantidad_proceso"]:
    if col in df_procesos.columns:
        df_procesos[col] = pd.to_numeric(df_procesos[col], errors='coerce').fillna(0)

# Convertir tipos de datos 
dtype_map = {}
if "id_proceso" in df_procesos.columns:
    dtype_map["id_proceso"] = "string"
if "id_producto" in df_procesos.columns:
    dtype_map["id_producto"] = "string"
for col in ["cantidad_recepcion", "cantidad_rechazada", "cantidad_proceso"]:
    if col in df_procesos.columns:
        dtype_map[col] = "int64"
if dtype_map:
    df_procesos = df_procesos.astype(dtype_map)

# Validar que las cantidades tengan coherencia 
if {"cantidad_recepcion", "cantidad_rechazada", "cantidad_proceso"}.issubset(df_procesos.columns):
    df_procesos["diferencia"] = df_procesos["cantidad_recepcion"] - df_procesos["cantidad_rechazada"]
    df_procesos["coherente"] = df_procesos["diferencia"] == df_procesos["cantidad_proceso"]
    inconsistentes = df_procesos[~df_procesos["coherente"]]
    if not inconsistentes.empty:
        print(" Registros con incoherencia entre recepción, rechazo y proceso:")
        print(inconsistentes[["id_proceso", "id_producto", "cantidad_recepcion", "cantidad_rechazada", "cantidad_proceso", "diferencia"]])
    df_procesos = df_procesos.drop(columns=["diferencia", "coherente"])

# Eliminar duplicados por Id_Proceso 
if "id_proceso" in df_procesos.columns:
    df_procesos = df_procesos.drop_duplicates(subset=["id_proceso"], keep="first")

# Detectar registros con datos faltantes
faltantes = df_procesos[df_procesos.isnull().any(axis=1)]
if not faltantes.empty:
    print(" Registros con datos faltantes tras la limpieza:")
    print(faltantes)

# Validar unicidad de la clave primaria si existe
if "id_proceso" in df_procesos.columns:
    assert df_procesos["id_proceso"].is_unique, "Error: existen Id_Proceso duplicados."

# Renombrar columnas 
rename_map = {
    "id_proceso": "Id_Proceso",
    "id_producto": "Id_Producto",
    "cantidad_recepcion": "Cantidad_Recepcion",
    "cantidad_rechazada": "Cantidad_Rechazada",
    "cantidad_proceso": "Cantidad_Proceso"
}
df_procesos = df_procesos.rename(columns={k: v for k, v in rename_map.items() if k in df_procesos.columns})

print("\n Tabla Hecho_Procesos limpia y transformada:")
print(df_procesos.head())

# Eliminar filas sin Id_Proceso
def eliminar_sin_id_proceso(df, col_id="Id_Proceso", mostrar_ejemplo=True):
    df_local = df.copy()
    # Asegurar columna para la comprobación
    if col_id in df_local.columns:
        df_local[col_id] = df_local[col_id].astype("string").str.strip()
    else:
        df_local[col_id] = pd.Series([pd.NA] * len(df_local), index=df_local.index, dtype="string")
    tiene_id = df_local[col_id].notna() & (df_local[col_id] != "")
    total_inicial = len(df_local)
    total_conservadas = int(tiene_id.sum())
    total_eliminadas = total_inicial - total_conservadas
    if mostrar_ejemplo and total_eliminadas > 0:
        print(f"\nSe eliminarán {total_eliminadas} filas que no tienen {col_id}. Ejemplo:")
        print(df_local.loc[~tiene_id].head(10))
    df_filtrado = df_local.loc[tiene_id].copy()
    return df_filtrado

# Aplicar la función antes de la carga
df_procesos = eliminar_sin_id_proceso(df_procesos, mostrar_ejemplo=True)

# (CARGA)
ruta_salida = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos\procesostransformados.csv"
with open(ruta_salida, mode='w', newline='', encoding='utf-8-sig') as archivo_csv:
    escritor = csv.writer(archivo_csv)
    escritor.writerow(df_procesos.columns)
    escritor.writerows(df_procesos.values.tolist())

print("\n Proceso ETL completado con éxito.")
print(f" Archivo CSV guardado en: {ruta_salida}")