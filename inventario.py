import streamlit as st
import pandas as pd
import io
import re

def detectar_y_renombrar_columnas(df):
    """
    Detecta y renombra las columnas necesarias basándose en nombres y contenido.
    
    Args:
        df (pd.DataFrame): El DataFrame a procesar.

    Returns:
        pd.DataFrame: El DataFrame con las columnas renombradas.
    """
    df_temp = df.copy()
    
    mapeo = {
        'CLAVE': ['CLAVE', 'ID', 'CODIGO', 'NUMERO'],
        'BASE': ['BASE'],
        'ALTURA': ['ALTURA'],
        'LATITUD': ['LATITUD', 'LAT', 'COORDENADA Y'],
        'LONGITUD': ['LONGITUD', 'LON', 'COORDENADA X'],
        'TARIFA PUBLICO': ['TARIFA PUBLICO', 'TARIFA RENTA', 'TARIFA'],
        'IMPRESION': ['IMPRESION', 'COSTO IMPRESION'],
        'INSTALACION': ['INSTALACION', 'COSTO INSTALACION']
    }

    nombres_renombrados = {}
    for nombre_estandar, nombres_alternativos in mapeo.items():
        for alt in nombres_alternativos:
            if alt in df_temp.columns:
                nombres_renombrados[alt] = nombre_estandar
                break

    df_temp.rename(columns=nombres_renombrados, inplace=True)
    
    columnas_disponibles = [col for col in df_temp.columns if col not in mapeo.keys()]
    for col in columnas_disponibles:
        try:
            valores_num = pd.to_numeric(df_temp[col], errors='coerce').dropna()
            if not valores_num.empty:
                max_val = valores_num.max()
                min_val = valores_num.min()
                if 'LATITUD' not in df_temp.columns and (-90 <= min_val <= max_val <= 90):
                    df_temp.rename(columns={col: 'LATITUD'}, inplace=True)
                elif 'LONGITUD' not in df_temp.columns and (-180 <= min_val <= max_val <= 180):
                    df_temp.rename(columns={col: 'LONGITUD'}, inplace=True)
        except Exception:
            pass
            
        patron_dimension = r'^\d+(\.\d+)?m?$'
        es_dimension = df_temp[col].astype(str).str.match(patron_dimension).sum() / len(df_temp) > 0.5
        if es_dimension:
            if 'BASE' not in df_temp.columns:
                df_temp.rename(columns={col: 'BASE'}, inplace=True)
            elif 'ALTURA' not in df_temp.columns:
                df_temp.rename(columns={col: 'ALTURA'}, inplace=True)

    df_temp.drop(columns=[col for col in df_temp.columns if col.startswith('Unnamed:')], inplace=True, errors='ignore')
    
    return df_temp

def corregir_coordenadas(df):
    """
    Corrige las coordenadas si están invertidas.
    """
    if 'LATITUD' in df.columns and 'LONGITUD' in df.columns:
        st.info("Verificando y corrigiendo coordenadas invertidas...")
        
        # Convierte las columnas a numérico, manejando errores
        df['LATITUD'] = pd.to_numeric(df['LATITUD'], errors='coerce')
        df['LONGITUD'] = pd.to_numeric(df['LONGITUD'], errors='coerce')

        # Itera sobre las filas para corregir
        for index, row in df.iterrows():
            lat = row['LATITUD']
            lon = row['LONGITUD']
            
            # Condición para detectar coordenadas invertidas
            if (lat < -180 or lat > 180) and (-90 <= lon <= 90):
                df.at[index, 'LATITUD'] = lon
                df.at[index, 'LONGITUD'] = lat
                st.write(f"Coordenadas corregidas en la fila {index}: {lat}, {lon} -> {lon}, {lat}")
                
    return df

def procesar_datos(df):
    """
    Procesa un DataFrame para calcular el área y los costos.
    """
    st.info("Detectando y renombrando columnas...")
    df_procesado = detectar_y_renombrar_columnas(df)

    if 'BASE' not in df_procesado.columns or 'ALTURA' not in df_procesado.columns:
        st.error("No se pudieron detectar las columnas 'BASE' y 'ALTURA'. Por favor, verifica el formato de tus datos.")
        return None

    try:
        for col in ['BASE', 'ALTURA', 'IMPRESION', 'INSTALACION', 'TARIFA PUBLICO', 'LATITUD', 'LONGITUD']:
            if col in df_procesado.columns:
                df_procesado[col] = df_procesado[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
                df_procesado[col] = pd.to_numeric(df_procesado[col], errors='coerce')
        
        # Se llama a la nueva función de corrección de coordenadas
        df_procesado = corregir_coordenadas(df_procesado)

        df_procesado.dropna(subset=['BASE', 'ALTURA'], inplace=True)
        
        df_procesado['AREA'] = df_procesado['BASE'] * df_procesado['ALTURA']
        
        # EL CAMBIO ESTÁ AQUÍ: SE USAN COLUMNAS 'IMPRESION' e 'INSTALACION'
        df_procesado['IMPRESION+INSTALACION'] = (
            df_procesado['AREA'].fillna(0) * df_procesado.get('IMPRESION', pd.Series(0)).fillna(0)
        ) + df_procesado.get('INSTALACION', pd.Series(0)).fillna(0)
        
        df_procesado = df_procesado.loc[:, ~df_procesado.columns.duplicated(keep='last')]
        
        return df_procesado

    except Exception as e:
        st.error(f"Ocurrió un error al procesar los datos: {e}")
        return None

# --- Interfaz de Streamlit ---
st.set_page_config(page_title="Gestor de Inventario y Publicidad", layout="wide")
st.title("Gestor de Inventario de Publicidad 📊")
st.markdown("Sube tu archivo de inventario principal (CSV) y el archivo con nuevos registros (Excel o CSV) para fusionarlos y procesarlos.")

st.subheader("Paso 1: Sube tus archivos")
file_principal = st.file_uploader("Inventario Principal (archivo original .csv)", type=["csv"])
file_nuevos = st.file_uploader("Nuevos Espectaculares (registros a añadir .csv/.xlsx)", type=["csv", "xlsx"])

if st.button("Fusionar y Procesar"):
    if file_principal is None or file_nuevos is None:
        st.warning("Por favor, sube ambos archivos para continuar.")
    else:
        try:
            # Leer el archivo principal
            df_principal = pd.read_csv(io.StringIO(file_principal.getvalue().decode("utf-8")))
            
            # Leer el archivo de nuevos registros
            file_extension = file_nuevos.name.split('.')[-1]
            if file_extension == 'csv':
                df_nuevos = pd.read_csv(io.StringIO(file_nuevos.getvalue().decode("utf-8")))
            elif file_extension == 'xlsx':
                df_nuevos = pd.read_excel(io.BytesIO(file_nuevos.getvalue()))
            else:
                st.error("Formato de archivo no soportado para 'Nuevos Espectaculares'.")
                df_nuevos = pd.DataFrame()

            if not df_nuevos.empty:
                # Filtrar columnas y fusionar
                df_nuevos_filtrado = df_nuevos[df_nuevos.columns.intersection(df_principal.columns)]
                df_final = pd.concat([df_principal, df_nuevos_filtrado], ignore_index=True)

                # Detección de duplicados
                df_final_clean = detectar_y_renombrar_columnas(df_final.copy())
                
                if 'CLAVE' in df_final_clean.columns:
                    df_final_clean['es_duplicado'] = df_final_clean.duplicated(subset=['CLAVE'], keep=False)
                elif 'LATITUD' in df_final_clean.columns and 'LONGITUD' in df_final_clean.columns:
                    df_final_clean['coordenadas'] = df_final_clean['LATITUD'].astype(str) + df_final_clean['LONGITUD'].astype(str)
                    df_final_clean['es_duplicado'] = df_final_clean.duplicated(subset=['coordenadas'], keep=False)
                    df_final_clean.drop(columns=['coordenadas'], inplace=True)
                else:
                    df_final_clean['es_duplicado'] = False
                    st.warning("No se pudo identificar una clave o coordenadas para detectar duplicados.")
                
                # Separar los datos
                df_procesados_final = df_final_clean[~df_final_clean['es_duplicado']].copy()
                df_agregados = df_procesados_final.tail(len(df_nuevos) - len(df_final_clean[df_final_clean['es_duplicado']]))
                df_omitidos = df_final_clean[df_final_clean['es_duplicado']].copy()

                df_procesado = procesar_datos(df_procesados_final)
                
                if df_procesado is not None:
                    st.success("Archivos fusionados y procesados exitosamente.")
                    
                    # Usar st.tabs para organizar la visualización
                    tab1, tab2, tab3 = st.tabs(["Base de Datos Completa", "Espectaculares Agregados", "Espectaculares Omitidos"])
                    
                    with tab1:
                        st.subheader("Base de Datos Completa ✅")
                        st.info(f"Total de registros: {len(df_procesado)}")
                        st.write(df_procesado)
                        
                        csv_data = df_procesado.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Descargar Inventario Final en CSV",
                            data=csv_data,
                            file_name="inventario_final.csv",
                            mime="text/csv",
                        )

                    with tab2:
                        st.subheader("Espectaculares Agregados ✨")
                        st.info(f"Se agregaron {len(df_agregados)} registros nuevos.")
                        st.write(df_agregados)

                    with tab3:
                        st.subheader("Espectaculares Omitidos ❌")
                        st.warning(f"Se omitieron {len(df_omitidos)} registros duplicados.")
                        st.write(df_omitidos)
                else:
                    st.error("El procesamiento de datos falló. Por favor, revisa tus archivos.")
        except Exception as e:
            st.error(f"Ocurrió un error al fusionar los archivos: {e}")

