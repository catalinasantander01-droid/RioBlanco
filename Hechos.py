import pandas as pd
import numpy as np
import unicodedata
from pathlib import Path

# --- 1. Rutas y Helpers (Tu código original) ---

# rutas
excel_path = Path(r'C:/Users/nicol/OneDrive/Documentos/SQL Server Management Studio/datos/datos.xlsx')
output_csv = Path(r'C:/Users/nicol/OneDrive/Documentos/SQL Server Management Studio/datos/hechostransformados.csv')

# leer todas las hojas
try:
    sheets = pd.read_excel(excel_path, sheet_name=None, dtype=str)
except FileNotFoundError:
    print(f"Error: No se encontró el archivo Excel en: {excel_path}")
    # En un entorno real, saldría aquí
    sheets = {}
except Exception as e:
    print(f"Error al leer el Excel: {e}")
    sheets = {}

# Normalizar nombres de hojas 
def norm_name(s: str) -> str:
    s2 = str(s).strip().lower()
    s2 = ''.join(ch for ch in unicodedata.normalize('NFKD', s2) if not unicodedata.combining(ch))
    s2 = s2.replace(' ', '').replace('-', '').replace('_', '')
    return s2

sheets_norm = {}
for name, df in sheets.items():
    df = df.copy()
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    sheets_norm[norm_name(name)] = df

# helper para obtener hoja
def get_sheet(options):
    for opt in options:
        n = norm_name(opt)
        if n in sheets_norm:
            return sheets_norm[n].copy()
    print(f"Advertencia: No se encontraron hojas para {options}")
    return pd.DataFrame()

# helpers de conversión
def to_dt(s, dayfirst=False):
    return pd.to_datetime(s, errors='coerce', dayfirst=dayfirst)

def to_num(s):
    return pd.to_numeric(s, errors='coerce')

def find_col(df: pd.DataFrame, candidates):
    if df is None or df.empty:
        return None
    def norm_col(c):
        s = str(c).strip().lower()
        s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
        s = s.replace(' ', '').replace('_','').replace('-','')
        return s
    cols_norm = {norm_col(c): c for c in df.columns}
    for cand in candidates:
        cand_norm = ''.join(ch for ch in cand.strip().lower() if ch.isalnum())
        for cn, original in cols_norm.items():
            if cand_norm in cn:
                return original
    return None

def _norm_str(s: object) -> str:
    if pd.isna(s):
        return ''
    s2 = str(s).strip().lower()
    s2 = ''.join(ch for ch in unicodedata.normalize('NFKD', s2) if not unicodedata.combining(ch))
    s2 = s2.replace(' ', '').replace('-', '').replace('_', '')
    return s2

# 2. Preparación de Hojas  

print("Cargando hojas...")
df_proveedores = get_sheet(['Proveedores'])
df_categorias  = get_sheet(['Categorias','Categorías'])
df_productos   = get_sheet(['Productos'])
df_procesos    = get_sheet(['Procesos'])
df_calidades   = get_sheet(['Calidades'])
df_frigorifico = get_sheet(['Frigorifico','Frigorífico','Frigorificos','Frigoríficos'])
df_cambios     = get_sheet(['Cambios'])
df_camaras     = get_sheet(['Camaras','Cámaras'])
df_fechas      = get_sheet(['Fechas'])
df_hechos      = get_sheet(['Hechos'])
df_lotes       = get_sheet(['Lote','Lotes'])
df_sucursales  = get_sheet(['Sucursal','Sucursales'])
df_packing     = get_sheet(['Packing','Packings'])

# 3. Preparación de Datos (Parseo de Fechas y Números)

print("Preparando datos (fechas y números)...")

# Parseo de fechas importantes
if not df_fechas.empty:
    df_fechas['recepcion'] = df_fechas.apply(lambda r: to_dt(f"{r.get('Dia','')} {r.get('Hora','')}"), axis=1)

if not df_packing.empty:
    col_pack_fecha = find_col(df_packing, ['Fecha_Salida','FechaSalida'])
    col_pack_hora  = find_col(df_packing, ['Hora_Salida','HoraSalida'])
    if col_pack_fecha:
        if col_pack_hora and col_pack_hora in df_packing.columns:
            df_packing['dt_packing_salida'] = df_packing.apply(lambda r: to_dt(f"{r.get(col_pack_fecha,'')} {r.get(col_pack_hora,'')}"), axis=1)
        else:
            df_packing['dt_packing_salida'] = to_dt(df_packing[col_pack_fecha])
    else:
        df_packing['dt_packing_salida'] = pd.NaT

if not df_lotes.empty:
    col_cosecha = find_col(df_lotes, ['FechaCosecha', 'Fecha_Cosecha'])
    if col_cosecha:
        df_lotes['Fecha_Cosecha'] = to_dt(df_lotes[col_cosecha])
    else:
        df_lotes['Fecha_Cosecha'] = pd.NaT

if not df_frigorifico.empty:
    df_frigorifico['dt_entrada'] = df_frigorifico.apply(lambda r: to_dt(f"{r.get('Fecha_entrada','')} {r.get('Hora_entrada','')}"), axis=1)
    df_frigorifico['dt_salida'] = df_frigorifico.apply(lambda r: to_dt(f"{r.get('Fecha_salida','')} {r.get('Hora_salida','')}"), axis=1)

# Parseo de valores numéricos
if not df_camaras.empty:
    col_cap = find_col(df_camaras, ['Capacidad'])
    if col_cap:
        df_camaras['Capacidad'] = to_num(df_camaras[col_cap])

if not df_procesos.empty:
    df_procesos['Cantidad_recepcion'] = to_num(df_procesos.get('Cantidad_recepcion'))
    df_procesos['Cantidad_proceso'] = to_num(df_procesos.get('Cantidad_proceso'))
    df_procesos['Cantidad_rechazada'] = to_num(df_procesos.get('Cantidad_rechazada'))

if not df_productos.empty:
    col_n_caja_prod = find_col(df_productos, ['N_caja'])
    if col_n_caja_prod:
        df_productos['N_caja'] = to_num(df_productos[col_n_caja_prod])
    else:
        df_productos['N_caja'] = np.nan

if not df_calidades.empty:
    col_bins_vaciados = find_col(df_calidades, ['Bins_vaciados','binsvaciados'])
    col_bins_recep  = find_col(df_calidades, ['Bins_recepcionados','binsrecepcionados'])
    col_cajas_cal = find_col(df_calidades, ['Cajas_calidad','cajascalidad'])
    
    df_calidades['Bins_vaciados'] = to_num(df_calidades.get(col_bins_vaciados))
    df_calidades['Bins_recepcionados'] = to_num(df_calidades.get(col_bins_recep))
    df_calidades['Cajas_calidad'] = to_num(df_calidades.get(col_cajas_cal))

# Limpieza de IDs
print("Limpiando IDs...")
all_dfs = {
    'hechos': (df_hechos, ['Id_Hecho','Id_Lote','Id_Frigorifico','Id_Fecha','Id_Packing','Id_Producto','SAG_Codigo','Id_Proceso','Id_Cambio']),
    'lotes': (df_lotes, ['Id_Lote']),
    'frigorifico': (df_frigorifico, ['Id_Frigorifico','Id_Camara']),
    'productos': (df_productos, ['Id_Producto']),
    'calidades': (df_calidades, ['SAG_Codigo']),
    'procesos': (df_procesos, ['Id_Proceso','Id_Producto']),
    'packing': (df_packing, ['Id_Packing','Id_Sucursal']),
    'fechas': (df_fechas, ['Id_Fecha']),
    'camaras': (df_camaras, ['Id_Camara']),
    'sucursales': (df_sucursales, ['Id_Sucursal']),
    'cambios': (df_cambios, ['Id_Cambio'])
}

for name, (d, cols) in all_dfs.items():
    if isinstance(d, pd.DataFrame) and not d.empty:
        for c in cols:
            if c in d.columns:
                d[c] = d[c].astype(str).str.strip()
            else:
                print(f"Advertencia: Columna ID '{c}' no encontrada en hoja '{name}'")

# 4. Pre-cálculo Necesarios

cap_by_frig = pd.DataFrame()
if not df_frigorifico.empty and not df_camaras.empty and 'Id_Camara' in df_frigorifico.columns and 'Id_Camara' in df_camaras.columns:
    tmp = df_frigorifico[['Id_Frigorifico','Id_Camara']].merge(df_camaras[['Id_Camara','Capacidad']], on='Id_Camara', how='left')
    cap_by_frig = tmp.groupby('Id_Frigorifico', as_index=False)['Capacidad'].sum().rename(columns={'Capacidad':'Capacidad_total'})


# 5. Construcción de Tabla de Hechos 

if df_hechos.empty:
    print("Error fatal: La hoja 'Hechos' está vacía. No se puede continuar.")
else:
    print("Iniciando construcción de tabla de hechos...")
    df_fact = df_hechos.copy()

    # Columnas de dimensiones
    base_cols = ['Id_Hecho', 'SAG_Codigo', 'Id_Cambio', 'Id_Lote', 'Id_Proceso','Id_Fecha','Id_Packing','Id_Frigorifico']
    
    # 5.1. Merge
    print("Fusionando dimensiones...")
    if not df_fechas.empty:
        df_fact = df_fact.merge(df_fechas[['Id_Fecha', 'recepcion']].drop_duplicates(subset=['Id_Fecha']), on='Id_Fecha', how='left')
    
    if not df_frigorifico.empty:
        df_fact = df_fact.merge(df_frigorifico[['Id_Frigorifico', 'dt_entrada', 'dt_salida']].drop_duplicates(subset=['Id_Frigorifico']), on='Id_Frigorifico', how='left')

    if not df_procesos.empty:
        cols_proc = ['Id_Proceso','Cantidad_recepcion','Cantidad_proceso','Cantidad_rechazada','Id_Producto']
        df_fact = df_fact.merge(df_procesos[cols_proc].drop_duplicates(subset=['Id_Proceso']), on='Id_Proceso', how='left')

    if not df_productos.empty:
        df_fact = df_fact.merge(df_productos[['Id_Producto', 'N_caja']].drop_duplicates(subset=['Id_Producto']), on='Id_Producto', how='left')

    if not df_calidades.empty:
        cols_cal = ['SAG_Codigo', 'Bins_recepcionados', 'Bins_vaciados', 'Cajas_calidad']
        df_fact = df_fact.merge(df_calidades[cols_cal].drop_duplicates(subset=['SAG_Codigo']), on='SAG_Codigo', how='left')

    if not df_packing.empty:
        col_tipo_pack = find_col(df_packing, ['Tipo_salida','TipoSalida'])
        pack_cols = ['Id_Packing', 'dt_packing_salida']
        if col_tipo_pack: pack_cols.append(col_tipo_pack)
        
        df_fact = df_fact.merge(df_packing[pack_cols].drop_duplicates(subset=['Id_Packing']), on='Id_Packing', how='left')
        
        # Renombrar columna de tipo de salida para consistencia
        if col_tipo_pack and col_tipo_pack in df_fact.columns and col_tipo_pack != 'Tipo_salida':
             df_fact = df_fact.rename(columns={col_tipo_pack: 'Tipo_salida'})

    if not df_cambios.empty:
        df_fact = df_fact.merge(df_cambios[['Id_Cambio', 'Nombre_error']].drop_duplicates(subset=['Id_Cambio']), on='Id_Cambio', how='left')

    if not cap_by_frig.empty:
        df_fact = df_fact.merge(cap_by_frig, on='Id_Frigorifico', how='left')


    # 6. Cálculo de Indicadores  
    print("Calculando indicadores...")

    # Función general para calcular días
    def calcular_dias(df, col_fin, col_inicio):
        return (df[col_fin] - df[col_inicio]).dt.total_seconds() / 86400

    # 1. Tratamiento (Recepción a Frigo)
    df_fact['Tratamiento'] = np.where(
        df_fact['recepcion'].notna() & df_fact['dt_entrada'].notna(),
        (df_fact['dt_entrada'] - df_fact['recepcion']).dt.total_seconds() / 86400,
        np.nan
    ).round(3)
    
    # 2. Productividad (Rendimiento)
    df_fact['Productividad'] = np.where(
        df_fact['Cantidad_recepcion'].gt(0),
        (df_fact['Cantidad_proceso'] / df_fact['Cantidad_recepcion']) * 100,
        np.nan
    ).round(2)

    # 3. Rechazo
    df_fact['Rechazo'] = np.where(
        df_fact['Cantidad_recepcion'].gt(0),
        (df_fact['Cantidad_rechazada'] / df_fact['Cantidad_recepcion']) * 100,
        np.nan
    ).round(2)

    # 4. Sanitario (Cajas Calidad / N_caja)
    df_fact['Sanitario'] = np.where(
        df_fact['N_caja'].gt(0),
        (df_fact['Cajas_calidad'] / df_fact['N_caja']) * 100,
        np.nan
    ).round(2)

    # 5. Indicador_Bins (Vaciados / Recepcionados)
    df_fact['Indicador_Bins'] = np.where(
        df_fact['Bins_recepcionados'].gt(0),
        (df_fact['Bins_vaciados'] / df_fact['Bins_recepcionados']) * 100,
        np.nan
    ).round(2)

    # 2. Preparar fechas del Ciclo
    df_fechas['dt_ciclo_inicio'] = pd.to_datetime(df_fechas['Dia'] + ' ' + df_fechas['Hora'], errors='coerce')
    df_fechas_slim = df_fechas[['Id_Fecha', 'dt_ciclo_inicio']]

    df_packing['dt_ciclo_fin'] = pd.to_datetime(
        df_packing['Fecha_Salida'] + ' ' + df_packing['Hora_Despacho'], errors='coerce'
    )
    df_packings_slim = df_packing[['Id_Packing', 'dt_ciclo_fin']]

    # 3. Preparar fechas de Enfriamiento
    df_frigorifico['dt_enfriamiento_inicio'] = pd.to_datetime(
        df_frigorifico['Fecha_entrada'] + ' ' + df_frigorifico['Hora_entrada'], errors='coerce'
    )
    df_frigorifico['dt_enfriamiento_fin'] = pd.to_datetime(
        df_frigorifico['Fecha_salida'] + ' ' + df_frigorifico['Hora_salida'], errors='coerce'
    )
    df_frigos_slim = df_frigorifico[['Id_Frigorifico', 'dt_enfriamiento_inicio', 'dt_enfriamiento_fin']]

    # 4. Unir todo a la tabla de hechos
    df_fact = df_fact.merge(df_fechas_slim, on='Id_Fecha', how='left')
    df_fact = df_fact.merge(df_packings_slim, on='Id_Packing', how='left')
    df_fact = df_fact.merge(df_frigos_slim, on='Id_Frigorifico', how='left')

    # 5. Calcular indicadores de duración en días
    df_fact['Ciclo'] = calcular_dias(df_fact, 'dt_ciclo_fin', 'dt_ciclo_inicio').round(3)
    df_fact['Enfriamiento'] = calcular_dias(df_fact, 'dt_enfriamiento_fin', 'dt_enfriamiento_inicio').round(3)
    
    
    # 8. Capacidad (Uso de Frigo)
    df_fact['Capacidad'] = np.where(
        df_fact['Capacidad_total'].gt(0),
        (df_fact['Cantidad_proceso'] / df_fact['Capacidad_total']) * 100.0,
        np.nan
    ).round(3)


    # 9. Exportar (Binario 0/1)
    ev = ['Maritima', 'Marítima', 'Terrestre', 'Multimodal', 'Aerea'] # Tu lista original omitía Aerea
    export_set = { _norm_str(x) for x in ev }
    
    df_fact['Tipo_salida_norm'] = df_fact.get('Tipo_salida', pd.Series(np.nan, index=df_fact.index)).apply(_norm_str)
    df_fact['es_exportacion'] = df_fact['Tipo_salida_norm'].isin(export_set)
    df_fact['Exportar'] = df_fact['es_exportacion'].astype(int)
    
    # (Columna extra de tu script, útil para análisis)
    df_fact['kg_exportado'] = np.where(df_fact['es_exportacion'], df_fact['Cantidad_proceso'], 0.0)


    # 10. Error (Binario 0/1)
    df_fact['Error'] = np.where(
        df_fact['Nombre_error'].astype(str).str.strip() != 'Sin error', 1, 0
    )


    # 7. Resultado final y Guardado 
    
    final_cols = [
        'Id_Hecho', 'SAG_Codigo', 'Id_Cambio', 'Id_Lote', 'Id_Proceso','Id_Fecha','Id_Packing','Id_Frigorifico',
        'Tratamiento','Productividad','Rechazo','Sanitario','Indicador_Bins','Ciclo',
        'Capacidad','Enfriamiento','Exportar','Error',
    ]
    
    final_cols_existing = [c for c in final_cols if c in df_fact.columns]
    
    missing_ids = [c for c in base_cols if c not in final_cols_existing and c in df_fact.columns]
    
    df_final = df_fact[final_cols_existing].copy()

    # Guardar CSV
    try:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(output_csv, index=False, encoding='utf-8-sig')
        
        print("\n Proceso completado!")
        print(f"Tabla de hechos guardada en: {output_csv}")
        print(f"Total de filas (Hechos): {len(df_final)}")
        print(f"Columnas de indicadores generadas: {len(final_cols) - len(base_cols)}")
        
        print("\n Resumen de Indicadores (Promedios)")
        for col in ['Tratamiento','Productividad','Rechazo','Sanitario','Indicador_Bins','Ciclo','Capacidad','Enfriamiento','Exportar','Error']:
            if col in df_final.columns:
                mean_val = pd.to_numeric(df_final[col], errors='coerce').mean()
                print(f"{col:<16}: {mean_val:.2f}")

    except Exception as e:
        print(f"\nError al guardar el archivo CSV: {e}")