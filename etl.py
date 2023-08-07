import pandas as pd
import pyodbc
import logging
import datetime
import requests
import os

def weekly_path():
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    weekly_csv_file_path = f"data/input/nuevas_filas_{today}.csv"
    clean_csv_path = f"data/input/cleaned_data_{today}.csv"
    csv_to_sql_log_path = f"data/logs/csv_to_sql_log_{today}.txt"
    data_quality_log_path = f"data/logs/data_quality_log_{today}.txt"
    
    return weekly_csv_file_path, clean_csv_path, csv_to_sql_log_path, data_quality_log_path

def download_csv(weekly_csv_file_path):
    # Definir la URL y la ruta local del archivo CSV
    csv_url = "https://adlssynapsetestfrancis.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2023-04-20T15:25:12Z&se=2023-12-31T23:25:12Z&spr=https&sv=2021-12-02&sr=b&sig=MZIobvBY6c7ht%2FdFLhtyJ3MZgqa%2B75%2BY3YWntqL%2FStI%3D"

    # Descargar el archivo CSV
    response = requests.get(csv_url)
    if response.status_code == 200:
        with open(weekly_csv_file_path, "wb") as csv_file:
            csv_file.write(response.content)
            logging.info("Archivo CSV descargado exitosamente.")
    else:
        # Poner Raise exception
        logging.info("Error al descargar el archivo CSV.")

def perform_data_quality_checks(input_file, clean_csv_path, data_quality_log_path):
    # Leer el archivo CSV y realizar chequeos de calidad
    problematic_chars = ['\n', '<', '>', '&', '#', '%', '\r\n']
    relevant_columns = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT', 'MUESTRA', 'VALOR', 'ORIGEN', 'RESULTADO']
    
    df = pd.read_csv(input_file, delimiter=',')
    issues_found = False
    pos_set = set()
    log_messages = []
    
    for i, row in df.iterrows():
        original_row = row.copy()
    
        for char in problematic_chars:
            for column in df.columns:
                if df[column].dtype == 'object':
                    modified_column = df[column].str.replace(char, '', n=1, regex=True)
                    df[column] = modified_column  # Actualizar la columna en el DataFrame


        # Chequeo de valores nulos en columnas relevantes
        for column in relevant_columns:
            if pd.isnull(row[column]):
                issues_found = True
                df.loc[i, column] = 'N/A'
                log_messages.append(f"Valor nulo reemplazado por 'N/A' en fila {i+1}, columna {column}")
        
        # Chequeo de duplicados basado en columnas relevantes
        unique_key = tuple(row[['ID', 'MUESTRA', 'RESULTADO']])
        if unique_key in pos_set:
            issues_found = True
            log_messages.append(f"Fila duplicada eliminada en fila {i+1}")
            df.drop(i, inplace=True)
            continue
        pos_set.add(unique_key)

        # Registrar cambios en el archivo de registro
        if not row.equals(original_row):
            log_messages.append(f"Modificación realizada en fila {i+1}: {original_row} -> {row.tolist()}")
    
    # Crear un nuevo archivo CSV con los datos corregidos si se encontraron problemas
    if issues_found:
        df.to_csv(clean_csv_path, index=False)
        
        # Registrar mensajes de registro en el archivo de registro
        with open(data_quality_log_path, 'w') as log:
            log.write("\n".join(log_messages))
        
        return clean_csv_path, data_quality_log_path
    
    return None, None


def load_csv_to_sql(csv_file_path, server_name, database_name, csv_to_sql_log_path):
    try:
        # Configuración de la conexión a SQL Server
        trusted_connection = 'yes'  # Para autenticación de Windows
        connection_string = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};TRUSTED_CONNECTION={trusted_connection}'

        # Cargar el archivo CSV en un DataFrame de pandas
        data = pd.read_csv(csv_file_path, sep=',')

        # Conectar a la base de datos
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        batch_size = 1000

        # Insertar los datos en lotes
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i:i+batch_size]
            
            insert_query = f'''
                INSERT INTO Unificado (
                    CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, MUESTRA, VALOR, ORIGEN, FECHA_COPIA, RESULTADO
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)
            '''
            
            for _, row in batch.iterrows():
                params = (
                    row["CHROM"],
                    row["POS"],
                    row["ID"],
                    row["REF"],
                    row["ALT"],
                    row["QUAL"],
                    row["FILTER"],
                    row["INFO"],
                    row["FORMAT"],
                    row["MUESTRA"],
                    row["VALOR"],
                    row["ORIGEN"],
                    row["RESULTADO"]
                )
                
                cursor.execute(insert_query, params)

            conn.commit()

            num_rows_affected = batch.shape[0] 

        # Obtener la fecha y hora actual del proceso
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Obtener el nombre de la instancia de SQL Server
        instance_name = conn.getinfo(pyodbc.SQL_SERVER_NAME)

        # Verificar la cantidad de filas en el CSV y en la tabla
        csv_row_count = len(data)
        table_row_count_query = "SELECT COUNT(*) FROM dbo.Unificado"
        cursor.execute(table_row_count_query)
        table_row_count = cursor.fetchone()[0]
    
    except Exception as e:
        # En caso de error, realizar rollback
        conn.rollback()
        logging.info("Ocurrió un error:", str(e))

    finally:
        # Cerrar la conexión
        conn.close()

    # Crear el mensaje del log
    log_message = (
        f"\nProceso de carga completado:\n"
        f" - Cantidad de filas afectadas: {num_rows_affected}\n"
        f" - Fecha del proceso: {current_datetime}\n"
        f" - Instancia de base de datos: {instance_name}\n"
        f" - Cantidad de filas en el CSV: {csv_row_count}\n"
        f" - Cantidad de filas en la tabla: {table_row_count}\n\n"
    )

    # Escribir el log en un archivo
    with open(csv_to_sql_log_path, 'a') as log_file:
        log_file.write(log_message)

def remove_duplicate_rows(database_name, server_name, csv_to_sql_log_path):
    # Configuración de la conexión a SQL Server
    trusted_connection = 'yes'  # Para autenticación de Windows
    connection_string = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};TRUSTED_CONNECTION={trusted_connection}'

    # Conectar a la base de datos
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Consulta SQL para recuperar filas duplicadas
    sql_query = '''
    WITH CTE AS (
        SELECT
            [ID],
            [MUESTRA],
            [RESULTADO],
            [FECHA_COPIA],
            ROW_NUMBER() OVER (PARTITION BY ID, MUESTRA, RESULTADO ORDER BY FECHA_COPIA DESC) AS RowNum
        FROM
            [dbo].[Unificado]
    )
    SELECT * FROM CTE WHERE RowNum > 1;
    '''
    
    try:
        # Ejecutar la consulta
        cursor.execute(sql_query)

        # Obtener los resultados
        duplicates = cursor.fetchall()

        # Contar filas duplicadas antes de la eliminación
        rows_affected_before = len(duplicates)

        # Eliminar las filas duplicadas
        for row in duplicates:
            delete_query = f'''
            DELETE FROM [dbo].[Unificado]
            WHERE [ID] = ? AND [MUESTRA] = ? AND [RESULTADO] = ?;
            '''
            cursor.execute(delete_query, (row.ID, row.MUESTRA, row.RESULTADO))
            conn.commit()

        # Contar filas totales después de la eliminación
        cursor.execute("SELECT COUNT(*) FROM [dbo].[Unificado];")
        total_rows_after = cursor.fetchone()[0]
    except Exception as e:
        # En caso de error, realizar rollback
        conn.rollback()
        logging.info("Ocurrió un error:", str(e))

    finally:
        # Cerrar la conexión
        cursor.close()
        conn.close()

    # Crear el mensaje del log
    log_message = (
        f"\nProceso de eliminacion de duplicados completado:\n"
        f" - Cantidad de filas duplicadas: {rows_affected_before}\n"
        f" - Cantidad de filas en la tabla: {total_rows_after}\n\n"
    )

    # Escribir el log en un archivo
    with open(csv_to_sql_log_path, 'a') as log_file:
        log_file.write(log_message)

def main():
    # Obtener rutas CSV y Logs
    weekly_csv_file_path, clean_csv_path, csv_to_sql_log_path, data_quality_log_path = weekly_path()

    # Configuracion de logs
    
    # Log resultados de la subida de datos a DB
    logging.basicConfig(filename=csv_to_sql_log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Log resultados de la revision del CSV
    logging.basicConfig(filename='data/logs/data_quality_log_path.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Descargar el archivo CSV
    download_csv(weekly_csv_file_path)

    # Realizar chequeos de calidad en el archivo CSV
    perform_data_quality_checks(weekly_csv_file_path, clean_csv_path, data_quality_log_path)

    # Cargar los datos limpios a la base de datos
    server_name = "JULICHIMENTE"
    database_name = "Testing_ETL"
    
    if not os.path.exists(clean_csv_path):
        clean_csv_path = weekly_csv_file_path
        logging.info("No se encontraron problemas en el archivo de entrada.")
    
    load_csv_to_sql(weekly_csv_file_path, server_name, database_name, csv_to_sql_log_path)
    
    # Revisar si existen rows duplicadas posterior a la carga
    remove_duplicate_rows(database_name, server_name, csv_to_sql_log_path)

    logging.info("Proceso ETL completado exitosamente.")

if __name__ == "__main__":
    main()
    