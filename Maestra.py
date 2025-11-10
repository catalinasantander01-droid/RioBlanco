import pandas as pd
import pyodbc
import os
import subprocess
import schedule
import time
from datetime import datetime


# Conexión a SQL Server

def get_connection(server: str, database: str):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

SERVER = r"Milagro\SQLEXPRESS"
DATABASE = "RIOBlancoDB"
conn = get_connection(SERVER, DATABASE)
cursor = conn.cursor()

# Rutas y archivos CSV

BASE_PATH = r"C:\Users\nicol\OneDrive\Documentos\SQL Server Management Studio\datos"

TABLAS_CSV = {
    "Dim_Lotes": "lotestransformados.csv",
    "Dim_Cambios": "cambiostransformados.csv",
    "Dim_Categorias": "categoriastransformados.csv",
    "Dim_Packings": "packingstransformados.csv",
    "Dim_Frigorificos": "frigorificostransformados.csv",
    "Dim_Fechas": "fechastransformados.csv",
    "Dim_Calidades": "calidadestransformados.csv",
    "Dim_Camaras": "camarastransformados.csv",
    "Dim_Procesos": "procesostransformados.csv",
    "Dim_Proveedores": "proveedorestransformados.csv",
    "Dim_Sucursales": "sucursalestransformados.csv",
    "Hechos": "hechostransformados.csv"
}

# Claves primarias 
CLAVES = {
    "Dim_Lotes": "Id_Lote",
    "Dim_Cambios": "Id_Cambio",
    "Dim_Categorias": "Id_Categoria",
    "Dim_Packings": "Id_Packing",
    "Dim_Frigorificos": "Id_Frigorifico",
    "Dim_Fechas": "Id_Fecha",
    "Dim_Calidades": "Id_Calidad",
    "Dim_Camaras": "Id_Camara",
    "Dim_Procesos": "Id_Proceso",
    "Dim_Proveedores": "Id_Proveedor",
    "Dim_Sucursales": "Id_Sucursal",
    "Hechos": "Id_Hecho"
}


# Función incremental

def cargar_csv_incremental(tabla, archivo_csv, columna_clave):
    ruta_csv = os.path.join(BASE_PATH, archivo_csv)
    if not os.path.exists(ruta_csv):
        print(f"Archivo no encontrado: {ruta_csv}")
        return

    # Leer el CSV
    df = pd.read_csv(ruta_csv, encoding="utf-8-sig")

    if df.empty:
        print(f" El archivo {archivo_csv} está vacío, no se cargará en {tabla}.")
        return

    # Verificar que la columna clave exista
    if columna_clave not in df.columns:
        print(f"La columna clave '{columna_clave}' no se encuentra en {archivo_csv}.")
        return

    # Obtener claves existentes en SQL Server
    try:
        cursor.execute(f"SELECT {columna_clave} FROM {tabla}")
        claves_sql = [fila[0] for fila in cursor.fetchall()]
    except Exception as e:
        print(f" Error al obtener claves de {tabla}: {e}")
        return

    # Filtrar registros nuevos a insertar
    nuevos_registros = df[~df[columna_clave].isin(claves_sql)]

    if nuevos_registros.empty:
        print(f"No hay nuevos datos para insertar en {tabla}.")
        return

    # Preparar columnas y placeholders
    columnas = ",".join(nuevos_registros.columns)
    placeholders = ",".join(["?"] * len(nuevos_registros.columns))

    # Insertar nuevos registros
    try:
        for fila in nuevos_registros.itertuples(index=False):
            cursor.execute(f"INSERT INTO {tabla} ({columnas}) VALUES ({placeholders})", *fila)
        conn.commit()
        print(f"{len(nuevos_registros)} nuevos registros insertados en {tabla}.")
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar en {tabla}: {e}")

# Ejecutar ETL y carga incremental
def ejecutar_etl_y_actualizar():
    print(f"\n Iniciando ETL a las {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")

    etl_scripts = [
        "Calidades.py", "Cambios.py", "Categorias.py", "Camaras.py",
        "Fechas.py", "Frigorificos.py", "Lotes.py", "Packings.py",
        "Procesos.py", "Proveedores.py", "Productos.py",
        "Sucursales.py", "Hechos.py"
    ]

    # Ejecutar scripts ETL (transformaciones)
    for script in etl_scripts:
        ruta_script = os.path.join(BASE_PATH, script)
        if os.path.exists(ruta_script):
            subprocess.run(["python", ruta_script], check=False)
        else:
            print(f"Script no encontrado: {ruta_script}")

    # Cargar CSVs de forma incremental
    for tabla, archivo in TABLAS_CSV.items():
        if tabla in CLAVES:
            cargar_csv_incremental(tabla, archivo, CLAVES[tabla])
        else:
            print(f"No se encontró clave primaria para {tabla}, se omitirá.")

    print(f" ETL completado correctamente ({datetime.now().strftime('%H:%M:%S')})")

# Programar ejecución diaria

schedule.every().day.at("20:00").do(ejecutar_etl_y_actualizar)

print("Proceso programado: actualización diaria a las 20:00 horas.")

while True:
    schedule.run_pending()
    time.sleep(30)
