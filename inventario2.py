import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from datetime import datetime  # Importar datetime para generar nombre único

def generar_nombre_unico(extension="csv"):
    """
    Genera un nombre único para el archivo descargable usando la fecha y hora actual.
    
    Args:
        extension (str): Extensión del archivo (por defecto 'csv')
    
    Returns:
        str: Nombre único del archivo
    """
    ahora = datetime.now()
    nombre_unico = f"inventario_{ahora.strftime('%Y%m%d_%H%M%S')}.{extension}"
    return nombre_unico

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
        'CLAVE': ['CLAVE', 'ID', 'CODIGO', 'NUMERO', "Clave"],
        'BASE': ['BASE', "Base"],
        'ALTURA': ['ALTURA', "Altura"],
        'LATITUD': ['LATITUD', 'LAT', 'COORDENADA Y', "Latitud"],
        'LONGITUD': ['LONGITUD', 'LON', 'COORDENADA X', "Longitud"],
        'TARIFA PUBLICO': ['TARIFA PUBLICO', 'TARIFA RENTA', 'TARIFA', "Tarifa", "TARIFA LISTA", "Tarifa Lista"],
        'IMPRESION': ['IMPRESION', 'COSTO IMPRESION', "Impresión"],
        'INSTALACION': ['INSTALACION', 'COSTO INSTALACION', "Instalación"],
        "CIUDAD": ["Ciudad", "Estado"],
        "MUNICIPIO": ["DELEGACIÓN/MUNICIPIO", "Delegacion/Municipio", "Delegación", "Delegacion", "Municipio"],
        "VISTA": ["Vista"],
        "DIRECCION": ["Direccion", "Dirección", "Ubicacion", "DIRECCIÓN"],
        "TIPO": ["TIPO", "Tipo"],
        "AREA": ["ÁREA", "área", "Área", "Area"],
        "PROVEEDOR": ["Proveedor"],
        "TELÉFONO PROVEEDOR": ["TELEFONO PROVEEDOR", "TELÉFONO", "TELEFONO", "Teléfono proveedor", "Teléfono Proveedor", "Teléfono"]
    }

    nombres_renombrados = {}
    for nombre_estandar, nombres_alternativos in mapeo.items():
        for alt in nombres_alternativos:
            if alt in df_temp.columns:
                nombres_renombrados[alt] = nombre_estandar
                break

    df_temp.rename(columns=nombres_renombrados, inplace=True)
   
    # Detectar automáticamente coordenadas basado en los valores
    columnas_numericas = df_temp.select_dtypes(include=[np.number]).columns
    for col in columnas_numericas:
        if col in ['LATITUD', 'LONGITUD', 'BASE', 'ALTURA', 'AREA']:
            continue
           
        valores = df_temp[col].dropna()
        if len(valores) > 0:
            min_val = valores.min()
            max_val = valores.max()
           
            # Si los valores están en rango de latitud (-90 a 90) y no tenemos LATITUD
            if 'LATITUD' not in df_temp.columns and (-90 <= min_val <= max_val <= 90):
                df_temp.rename(columns={col: 'LATITUD'}, inplace=True)
                st.info(f"Columna '{col}' detectada como LATITUD basado en valores")
           
            # Si los valores están en rango de longitud (-180 a 180) y no tenemos LONGITUD
            elif 'LONGITUD' not in df_temp.columns and (-180 <= min_val <= max_val <= 180):
                df_temp.rename(columns={col: 'LONGITUD'}, inplace=True)
                st.info(f"Columna '{col}' detectada como LONGITUD basado en valores")

    df_temp.drop(columns=[col for col in df_temp.columns if col.startswith('Unnamed:')], inplace=True, errors='ignore')
   
    return df_temp

def separar_medidas_combinadas(df):
    """
    Separa las medidas combinadas en formato '18x59' en columnas BASE y ALTURA separadas.

    Args:
        df (pd.DataFrame): El DataFrame a procesar.

    Returns:
        pd.DataFrame: El DataFrame con las medidas separadas.
    """
    df_temp = df.copy()
   
    if 'BASE' in df_temp.columns:
        # Patrón para detectar medidas combinadas: 18x59, 18*59, 18 x 59, 18 * 59, etc.
        patron_dimension_combinada = r'^\s*(\d+\.?\d*)\s*[x\*]\s*(\d+\.?\d*)\s*$'
       
        # Crear columnas temporales para base y altura
        base_values = []
        altura_values = []
        medidas_separadas = 0
       
        for index, row in df_temp.iterrows():
            base_val = row.get('BASE', '')
           
            if isinstance(base_val, str):
                match = re.match(patron_dimension_combinada, base_val.strip())
                if match:
                    # Si encuentra el patrón, separar en base y altura
                    base_values.append(float(match.group(1)))
                    altura_values.append(float(match.group(2)))
                    medidas_separadas += 1
                else:
                    # Si no encuentra el patrón, mantener valores originales
                    try:
                        base_values.append(float(base_val))
                    except:
                        base_values.append(pd.NA)
                    altura_values.append(pd.NA)
            else:
                # Si no es string, mantener valores originales
                base_values.append(base_val)
                altura_values.append(pd.NA)
       
        # Actualizar las columnas
        df_temp['BASE'] = base_values
       
        # Si se encontraron medidas combinadas, crear/actualizar columna ALTURA
        if medidas_separadas > 0:
            if 'ALTURA' not in df_temp.columns:
                df_temp['ALTURA'] = altura_values
            else:
                # Completar valores nulos en ALTURA con los valores separados
                for i, altura_val in enumerate(altura_values):
                    if altura_val is not pd.NA and (pd.isna(df_temp.at[i, 'ALTURA']) or df_temp.at[i, 'ALTURA'] == ''):
                        df_temp.at[i, 'ALTURA'] = altura_val
           
            st.info(f"Se separaron {medidas_separadas} medidas combinadas en BASE y ALTURA")
   
    return df_temp

def corregir_coordenadas(df):
    """
    Corrige las coordenadas si están invertidas y detecta automáticamente.
    """
    df_temp = df.copy()
   
    # Primero identificar qué columnas podrían ser coordenadas
    coord_columns = []
    for col in df_temp.columns:
        if any(keyword in col.upper() for keyword in ['LAT', 'LON', 'LONGITUD', 'LATITUD', 'COORD']):
            coord_columns.append(col)
   
    if len(coord_columns) >= 2:
        st.info(f"Columnas de coordenadas detectadas: {coord_columns}")
       
        # Convertir a numérico
        for col in coord_columns:
            df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce')
       
        # Verificar si las coordenadas están invertidas
        for i in range(len(coord_columns)):
            for j in range(i+1, len(coord_columns)):
                col1, col2 = coord_columns[i], coord_columns[j]
               
                # Contar cuántos valores están en el rango correcto
                lat_count = ((df_temp[col1] >= -90) & (df_temp[col1] <= 90)).sum()
                lon_count = ((df_temp[col2] >= -180) & (df_temp[col2] <= 180)).sum()
               
                # Si parece que están invertidas, corregir
                if lat_count < lon_count:
                    st.info(f"Posible inversión detectada entre {col1} y {col2}. Corrigiendo...")
                    df_temp[['LATITUD_TEMP', 'LONGITUD_TEMP']] = df_temp[[col2, col1]]
                    df_temp.drop(columns=[col1, col2], inplace=True)
                    df_temp.rename(columns={'LATITUD_TEMP': 'LATITUD', 'LONGITUD_TEMP': 'LONGITUD'}, inplace=True)
                    break
   
    return df_temp

def procesar_datos(df, columnas_originales):
    """
    Procesa un DataFrame para calcular el área y los costos, manteniendo solo las columnas originales.
    
    Args:
        df (pd.DataFrame): El DataFrame a procesar.
        columnas_originales (list): Lista de columnas que deben mantenerse en el resultado final.

    Returns:
        pd.DataFrame: El DataFrame procesado con solo las columnas originales.
    """
    st.info("Detectando y renombrando columnas...")
    df_procesado = detectar_y_renombrar_columnas(df)
   
    st.info("Separando medidas combinadas...")
    df_procesado = separar_medidas_combinadas(df_procesado)

    if 'BASE' not in df_procesado.columns:
        st.error("No se pudo detectar la columna 'BASE'. Por favor, verifica el formato de tus datos.")
        return None

    try:
        # Convertir a numérico
        df_procesado['BASE'] = pd.to_numeric(df_procesado['BASE'], errors='coerce')
       
        if 'ALTURA' in df_procesado.columns:
            df_procesado['ALTURA'] = pd.to_numeric(df_procesado['ALTURA'], errors='coerce')
       
        # Calcular área -
        if 'ALTURA' in df_procesado.columns and not df_procesado['ALTURA'].isna().all():
            # Solo calcular área si tenemos ambas dimensiones
            tiene_ambas = ~df_procesado['BASE'].isna() & ~df_procesado['ALTURA'].isna()
            df_procesado.loc[tiene_ambas, 'AREA'] = df_procesado.loc[tiene_ambas, 'BASE'] * df_procesado.loc[tiene_ambas, 'ALTURA']
            st.info(f"Área calculada para {tiene_ambas.sum()} registros con BASE y ALTURA válidas")
        else:
            # Si no hay altura válida, usar solo la base
            df_procesado['AREA'] = df_procesado['BASE']
            st.warning("No se encontró columna ALTURA válida. Usando BASE como área.")

        # Convertir otras columnas numéricas
        for col in ['IMPRESION', 'INSTALACION', 'TARIFA PUBLICO', 'LATITUD', 'LONGITUD']:
            if col in df_procesado.columns:
                df_procesado[col] = pd.to_numeric(df_procesado[col], errors='coerce')
       
        df_procesado = corregir_coordenadas(df_procesado)
       
        # Calcular costo total si las columnas existen
        if 'IMPRESION' in df_procesado.columns and 'INSTALACION' in df_procesado.columns:
            df_procesado['IMPRESION+INSTALACION'] = (
                df_procesado['AREA'].fillna(0) * df_procesado['IMPRESION'].fillna(0)
            ) + df_procesado['INSTALACION'].fillna(0)
            st.info("Costo de impresión + instalación calculado")
       
        # Eliminar columnas duplicadas
        df_procesado = df_procesado.loc[:, ~df_procesado.columns.duplicated(keep='last')]
       
        # Filtrar solo las columnas que estaban en el inventario original
        columnas_a_mantener = [col for col in columnas_originales if col in df_procesado.columns]
        df_procesado = df_procesado[columnas_a_mantener]
       
        # Mostrar estadísticas
        st.success(f"Procesamiento completado. Registros: {len(df_procesado)}, "
                  f"Con área calculada: {df_procesado['AREA'].notna().sum() if 'AREA' in df_procesado.columns else 0}")
       
        return df_procesado

    except Exception as e:
        st.error(f"Ocurrió un error al procesar los datos: {e}")
        import traceback
        st.error(traceback.format_exc())
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
            # Leer el archivo principal con manejo de codificación
            try:
                df_principal = pd.read_csv(file_principal, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df_principal = pd.read_csv(file_principal, encoding='latin-1')
                except:
                    st.error("No se pudo leer el archivo principal. Verifica la codificación.")
                    df_principal = pd.DataFrame()
           
            # Guardar las columnas originales del inventario principal
            columnas_originales = df_principal.columns.tolist()
            st.info(f"Columnas originales detectadas: {', '.join(columnas_originales)}")
           
            # Leer el archivo de nuevos registros
            file_extension = file_nuevos.name.split('.')[-1]
            if file_extension == 'csv':
                try:
                    df_nuevos = pd.read_csv(file_nuevos, encoding='utf-8')
                except UnicodeDecodeError:
                    try:
                        df_nuevos = pd.read_csv(file_nuevos, encoding='latin-1')
                    except:
                        st.error("No se pudo leer el archivo de nuevos registros. Verifica la codificación.")
                        df_nuevos = pd.DataFrame()
            elif file_extension == 'xlsx':
                df_nuevos = pd.read_excel(file_nuevos)
            else:
                st.error("Formato de archivo no soportado para 'Nuevos Espectaculares'.")
                df_nuevos = pd.DataFrame()

            if not df_nuevos.empty and not df_principal.empty:
                st.info(f"Archivo principal: {len(df_principal)} registros")
                st.info(f"Nuevos registros: {len(df_nuevos)} registros")
               
                # Renombrar las columnas de df_nuevos
                df_nuevos_renombrado = detectar_y_renombrar_columnas(df_nuevos.copy())
                # Separar medidas combinadas en los nuevos registros
                df_nuevos_renombrado = separar_medidas_combinadas(df_nuevos_renombrado)
               
                # Fusionar con el DataFrame principal
                df_final = pd.concat([df_principal, df_nuevos_renombrado], ignore_index=True)
                st.info(f"Después de fusionar: {len(df_final)} registros")

                # Detección de duplicados
                df_final_clean = df_final.copy()
               
                duplicados_encontrados = 0
                if 'CLAVE' in df_final_clean.columns:
                    duplicados = df_final_clean.duplicated(subset=['CLAVE'], keep='first')
                    df_final_clean['es_duplicado'] = duplicados
                    duplicados_encontrados = duplicados.sum()
               
                elif 'LATITUD' in df_final_clean.columns and 'LONGITUD' in df_final_clean.columns:
                    # Usar coordenadas redondeadas para detectar duplicados
                    df_final_clean['LAT_ROUND'] = df_final_clean['LATITUD'].round(4)
                    df_final_clean['LON_ROUND'] = df_final_clean['LONGITUD'].round(4)
                    duplicados = df_final_clean.duplicated(subset=['LAT_ROUND', 'LON_ROUND'], keep='first')
                    df_final_clean['es_duplicado'] = duplicados
                    duplicados_encontrados = duplicados.sum()
                    df_final_clean.drop(columns=['LAT_ROUND', 'LON_ROUND'], inplace=True)
               
                else:
                    df_final_clean['es_duplicado'] = False
                    st.warning("No se pudo identificar una clave o coordenadas para detectar duplicados.")
               
                st.info(f"Duplicados encontrados: {duplicados_encontrados}")
               
                # Separar los datos
                df_procesados_final = df_final_clean[~df_final_clean['es_duplicado']].copy()
                df_omitidos = df_final_clean[df_final_clean['es_duplicado']].copy()
               
                # Los agregados son los nuevos registros que no son duplicados
                df_agregados = df_nuevos_renombrado[~df_nuevos_renombrado.index.isin(df_omitidos.index)]
               
                st.info(f"Registros a procesar: {len(df_procesados_final)}")
                st.info(f"Registros agregados: {len(df_agregados)}")
                st.info(f"Registros omitidos (duplicados): {len(df_omitidos)}")

                # Procesar datos manteniendo solo las columnas originales
                df_procesado = procesar_datos(df_procesados_final, columnas_originales)
               
                if df_procesado is not None:
                    st.success("Archivos fusionados y procesados exitosamente.")
                   
                    # Generar nombre único para el archivo descargable
                    nombre_archivo = generar_nombre_unico()
                   
                    # Usar st.tabs para organizar la visualización
                    tab1, tab2, tab3 = st.tabs(["Base de Datos Completa", "Espectaculares Agregados", "Espectaculares Omitidos"])
                   
                    with tab1:
                        st.subheader("Base de Datos Completa ✅")
                        st.info(f"Total de registros: {len(df_procesado)}")
                        st.dataframe(df_procesado)
                       
                        csv_data = df_procesado.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                        st.download_button(
                            label="Descargar Inventario Final en CSV",
                            data=csv_data,
                            file_name=nombre_archivo,
                            mime="text/csv",
                        )

                    with tab2:
                        st.subheader("Espectaculares Agregados ✨")
                        st.info(f"Se agregaron {len(df_agregados)} registros nuevos.")
                        # Filtrar solo las columnas originales en los agregados
                        columnas_agregados = [col for col in columnas_originales if col in df_agregados.columns]
                        df_agregados_filtrado = df_agregados[columnas_agregados] if len(columnas_agregados) > 0 else df_agregados
                        st.dataframe(df_agregados_filtrado)

                    with tab3:
                        st.subheader("Espectaculares Omitidos ❌")
                        st.warning(f"Se omitieron {len(df_omitidos)} registros duplicados.")
                        # Filtrar solo las columnas originales en los omitidos
                        columnas_omitidos = [col for col in columnas_originales if col in df_omitidos.columns]
                        df_omitidos_filtrado = df_omitidos[columnas_omitidos] if len(columnas_omitidos) > 0 else df_omitidos
                        st.dataframe(df_omitidos_filtrado)
                else:
                    st.error("El procesamiento de datos falló. Por favor, revisa tus archivos.")
            else:
                st.error("No se pudieron cargar correctamente los archivos. Verifica el formato y la codificación.")
        except Exception as e:
            st.error(f"Ocurrió un error al fusionar los archivos: {e}")
            import traceback
            st.error(traceback.format_exc())