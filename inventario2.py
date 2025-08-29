import traceback
import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from datetime import datetime

def generar_nombre_unico(extension="csv"):
    """
    Genera un nombre único para el archivo descargable usando la fecha y hora actual.
    """
    ahora = datetime.now()
    nombre_unico = f"inventario_{ahora.strftime('%Y%m%d_%H%M%S')}.{extension}"
    return nombre_unico

def detectar_y_renombrar_columnas(df):
    """
    Detecta y renombra las columnas necesarias basándose en nombres y contenido.
    """
    df_temp = df.copy()
    
    # Normalizar los nombres de las columnas para una mejor detección
    df_temp.columns = [col.upper().strip() for col in df_temp.columns]

    mapeo = {
        'CLAVE': ['CLAVE', 'ID', 'CODIGO', 'NUMERO'],
        'BASE': ['BASE'],
        'ALTURA': ['ALTURA'],
        'LATITUD': ['LATITUD', 'LAT', 'COORDENADA Y'],
        'LONGITUD': ['LONGITUD', 'LON', 'COORDENADA X'],
        'TARIFA PUBLICO': ['TARIFA PUBLICO', 'TARIFA RENTA', 'TARIFA', 'TARIFA LISTA'],
        'IMPRESION': ['IMPRESION', 'COSTO IMPRESION'],
        'INSTALACION': ['INSTALACION', 'COSTO INSTALACION'],
        "CIUDAD": ["CIUDAD", "ESTADO"],
        "MUNICIPIO": ["DELEGACIÓN/MUNICIPIO", "DELEGACION/MUNICIPIO", "DELEGACIÓN", "DELEGACION", "MUNICIPIO"],
        "VISTA": ["VISTA"],
        "DIRECCION": ["DIRECCION", "DIRECCIÓN", "UBICACION"],
        "TIPO": ["TIPO"],
        "AREA": ["ÁREA", "AREA"],
        "PROVEEDOR": ["PROVEEDOR"],
        "TELÉFONO PROVEEDOR": ["TELEFONO PROVEEDOR", "TELÉFONO", "TELEFONO"]
    }

    nombres_renombrados = {}
    for nombre_estandar, nombres_alternativos in mapeo.items():
        for alt in nombres_alternativos:
            if alt in df_temp.columns:
                nombres_renombrados[alt] = nombre_estandar
                break
                
    df_temp.rename(columns=nombres_renombrados, inplace=True)
    df_temp = df_temp.loc[:, ~df_temp.columns.duplicated(keep='first')]
    
    columnas_numericas = df_temp.select_dtypes(include=[np.number]).columns
    for col in columnas_numericas:
        if col in ['LATITUD', 'LONGITUD', 'BASE', 'ALTURA', 'AREA']:
            continue
        valores = df_temp[col].dropna()
        if len(valores) > 0:
            min_val = valores.min()
            max_val = valores.max()
            if 'LATITUD' not in df_temp.columns and (-90 <= min_val <= max_val <= 90):
                df_temp.rename(columns={col: 'LATITUD'}, inplace=True)
                st.info(f"Columna '{col}' detectada como LATITUD basado en valores.")
            elif 'LONGITUD' not in df_temp.columns and (-180 <= min_val <= max_val <= 180):
                df_temp.rename(columns={col: 'LONGITUD'}, inplace=True)
                st.info(f"Columna '{col}' detectada como LONGITUD basado en valores.")

    df_temp.drop(columns=[col for col in df_temp.columns if col.startswith('UNNAMED:')], inplace=True, errors='ignore')
    
    return df_temp

def separar_medidas_combinadas(df):
    """
    Separa las medidas combinadas en formato '18x59' en columnas BASE y ALTURA separadas.
    """
    df_temp = df.copy()
    
    if 'BASE' in df_temp.columns:
        patron_dimension_combinada = r'^\s*(\d+\.?\d*)\s*[x\*]\s*(\d+\.?\d*)\s*$'
        dimensiones = df_temp['BASE'].astype(str).str.extract(patron_dimension_combinada, expand=True)
        
        if not dimensiones.empty and not dimensiones.isnull().all().all():
            st.info("Detectando y separando medidas combinadas en la columna BASE.")
            dimensiones.columns = ['BASE_temp', 'ALTURA_temp']
            dimensiones = dimensiones.apply(pd.to_numeric, errors='coerce')

            filas_con_medidas_combinadas = dimensiones['BASE_temp'].notna()

            df_temp.loc[filas_con_medidas_combinadas, 'BASE'] = dimensiones.loc[filas_con_medidas_combinadas, 'BASE_temp']

            if 'ALTURA' not in df_temp.columns:
                df_temp['ALTURA'] = pd.NA
            
            df_temp.loc[filas_con_medidas_combinadas, 'ALTURA'] = dimensiones.loc[filas_con_medidas_combinadas, 'ALTURA_temp']
            
            medidas_separadas = filas_con_medidas_combinadas.sum()
            if medidas_separadas > 0:
                st.info(f"Se separaron {medidas_separadas} medidas combinadas en BASE y ALTURA.")
            
    return df_temp

def corregir_coordenadas(df):
    """
    Corrige las coordenadas si están invertidas.
    """
    df_temp = df.copy()
    
    if 'LATITUD' in df_temp.columns and 'LONGITUD' in df_temp.columns:
        df_temp['LATITUD'] = pd.to_numeric(df_temp['LATITUD'], errors='coerce')
        df_temp['LONGITUD'] = pd.to_numeric(df_temp['LONGITUD'], errors='coerce')
        
        lat_valid = ((df_temp['LATITUD'] >= -90) & (df_temp['LATITUD'] <= 90)).sum()
        lon_valid = ((df_temp['LONGITUD'] >= -180) & (df_temp['LONGITUD'] <= 180)).sum()
        
        if (lat_valid < lon_valid) and (lon_valid > 0):
            st.info("Posible inversión de coordenadas detectada. Corrigiendo...")
            temp_lat = df_temp['LATITUD'].copy()
            df_temp['LATITUD'] = df_temp['LONGITUD']
            df_temp['LONGITUD'] = temp_lat
    
    return df_temp

def procesar_datos(df):
    """
    Procesa un DataFrame para calcular el área y los costos.
    """
    df_procesado = df.copy()
    
    df_procesado = separar_medidas_combinadas(df_procesado)
    
    if 'BASE' in df_procesado.columns:
        df_procesado['BASE'] = pd.to_numeric(df_procesado['BASE'], errors='coerce')
        if 'ALTURA' in df_procesado.columns:
            df_procesado['ALTURA'] = pd.to_numeric(df_procesado['ALTURA'], errors='coerce')
            df_procesado['AREA'] = df_procesado['BASE'] * df_procesado['ALTURA']
        else:
            df_procesado['AREA'] = df_procesado['BASE']
    
    for col in ['IMPRESION', 'INSTALACION', 'TARIFA PUBLICO', 'LATITUD', 'LONGITUD']:
        if col in df_procesado.columns:
            df_procesado[col] = pd.to_numeric(df_procesado[col], errors='coerce')
    
    df_procesado = corregir_coordenadas(df_procesado)
    
    if 'IMPRESION' in df_procesado.columns and 'INSTALACION' in df_procesado.columns and 'AREA' in df_procesado.columns:
        df_procesado['IMPRESION+INSTALACION'] = (
            df_procesado['AREA'].fillna(0) * df_procesado['IMPRESION'].fillna(0)
        ) + df_procesado['INSTALACION'].fillna(0)
    
    return df_procesado

def procesar_y_fusionar_archivos(file_principal, file_nuevos, proveedor, telefono_proveedor):
    """
    Función principal para leer, fusionar y procesar los DataFrames.
    """
    df_principal = pd.read_csv(file_principal, encoding='utf-8')
    df_nuevos = pd.read_excel(file_nuevos) if file_nuevos.name.endswith('.xlsx') else pd.read_csv(file_nuevos, encoding='utf-8')

    columnas_originales = [col.upper().strip() for col in df_principal.columns]
    df_principal.columns = columnas_originales
    df_nuevos.columns = [col.upper().strip() for col in df_nuevos.columns]

    if proveedor:
        df_nuevos['PROVEEDOR'] = proveedor
    if telefono_proveedor:
        df_nuevos['TELÉFONO PROVEEDOR'] = telefono_proveedor
    
    # 1. Renombrar y detectar columnas en ambos DataFrames antes de fusionar
    df_principal_procesado = detectar_y_renombrar_columnas(df_principal)
    df_nuevos_procesado = detectar_y_renombrar_columnas(df_nuevos)

    # 2. Identificar y separar duplicados
    df_fusion = pd.concat([df_principal_procesado, df_nuevos_procesado], ignore_index=True)
    duplicados_columns = [col for col in ['CLAVE', 'DIRECCION', 'LATITUD', 'LONGITUD'] if col in df_fusion.columns]

    if not duplicados_columns:
        st.warning("No se encontraron columnas clave para detectar duplicados.")
        df_final = df_fusion
        df_omitidos = pd.DataFrame()
    else:
        df_fusion['es_duplicado'] = df_fusion.duplicated(subset=duplicados_columns, keep='first')
        df_final = df_fusion[~df_fusion['es_duplicado']].drop(columns=['es_duplicado'])
        df_omitidos = df_fusion[df_fusion['es_duplicado']].drop(columns=['es_duplicado'])

    # 3. Aplicar el procesamiento de datos a los conjuntos finales
    df_final_procesado = procesar_datos(df_final)
    df_omitidos_procesado = procesar_datos(df_omitidos)
    df_nuevos_procesado = procesar_datos(df_nuevos)

    # 4. Filtrar por las columnas originales del archivo principal para la vista final
    columnas_disponibles_final = [col for col in columnas_originales if col in df_final_procesado.columns]
    df_final_procesado = df_final_procesado[columnas_disponibles_final]
    
    columnas_disponibles_agregados = [col for col in columnas_originales if col in df_nuevos_procesado.columns]
    df_nuevos_filtrado = df_nuevos_procesado[columnas_disponibles_agregados]
    
    columnas_disponibles_omitidos = [col for col in columnas_originales if col in df_omitidos_procesado.columns]
    df_omitidos_filtrado = df_omitidos_procesado[columnas_disponibles_omitidos]

    return df_final_procesado, df_nuevos_filtrado, df_omitidos_filtrado

# --- Interfaz de Streamlit ---
st.set_page_config(page_title="Gestor de Inventario y Publicidad", layout="wide")
st.title("Gestor de Inventario de Publicidad 📊")
st.markdown("Sube tu archivo de inventario principal (CSV) y el archivo con nuevos registros (Excel o CSV) para fusionarlos y procesarlos.")

st.subheader("Información del Proveedor para los Nuevos Espectaculares")
proveedor = st.text_input("Nombre del Proveedor")
telefono_proveedor = st.text_input("Teléfono del Proveedor")

st.subheader("Paso 1: Sube tus archivos")
file_principal = st.file_uploader("Inventario Principal (archivo original .csv)", type=["csv"])
file_nuevos = st.file_uploader("Nuevos Espectaculares (registros a añadir .csv/.xlsx)", type=["csv", "xlsx"])

if 'df_final' not in st.session_state:
    st.session_state.df_final = None
if 'df_omitidos' not in st.session_state:
    st.session_state.df_omitidos = pd.DataFrame()
if 'df_agregados' not in st.session_state:
    st.session_state.df_agregados = pd.DataFrame()

if st.button("Fusionar y Procesar"):
    if file_principal is None or file_nuevos is None:
        st.warning("Por favor, sube ambos archivos para continuar.")
    else:
        try:
            df_final, df_agregados, df_omitidos = procesar_y_fusionar_archivos(file_principal, file_nuevos, proveedor, telefono_proveedor)
            
            st.session_state.df_final = df_final
            st.session_state.df_agregados = df_agregados
            st.session_state.df_omitidos = df_omitidos
            
            st.success("Archivos fusionados y procesados exitosamente.")
            
        except Exception as e:
            st.error(f"Ocurrió un error al fusionar o procesar los archivos: {e}")
            st.error(traceback.format_exc())

# Sección de visualización y descarga
if st.session_state.df_final is not None and not st.session_state.df_final.empty:
    tab1, tab2, tab3 = st.tabs(["Base de Datos Completa", "Espectaculares Agregados", "Espectaculares Omitidos"])
    
    with tab1:
        st.subheader("Base de Datos Completa ✅")
        st.info(f"Total de registros: {len(st.session_state.df_final)}")
        st.dataframe(st.session_state.df_final)
        
        csv_data = st.session_state.df_final.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button(
            label="Descargar Inventario Final en CSV",
            data=csv_data,
            file_name=generar_nombre_unico(),
            mime="text/csv",
        )

    with tab2:
        st.subheader("Espectaculares Agregados ✨")
        if st.session_state.df_agregados is not None and not st.session_state.df_agregados.empty:
            st.info(f"Se agregaron {len(st.session_state.df_agregados)} registros nuevos.")
            st.dataframe(st.session_state.df_agregados)
        else:
            st.info("No se agregaron nuevos registros o todos fueron omitidos por duplicidad.")

    with tab3:
        st.subheader("Espectaculares Omitidos ❌")
        if st.session_state.df_omitidos is not None and not st.session_state.df_omitidos.empty:
            st.warning(f"Se omitieron {len(st.session_state.df_omitidos)} registros duplicados.")
            st.dataframe(st.session_state.df_omitidos)
        else:
            st.info("No se omitieron registros.")