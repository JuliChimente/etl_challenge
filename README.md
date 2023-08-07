# Explicación de la solución

 La solución se basa en un conjunto de scripts los cuales en diferentes pasos se encargan de descargar
el archivo csv semanalmente mediante el task scheduler de windows, luego de esto se analiza posibles 
conflictos en la data del csv, como nulos, duplicados, etc. Posterior a esto se genera la subida del csv
ya revisado a la tabla Unificado dentro de la DB Testing_ETL. Por ultimo se genera una ultima revision, en
este caso para corroborar que la tabla no contenga duplicados y en caso de contenerlos quedarse con los que
tienen ultima fecha de copia.

# Scripts utilizados

- weekly_path: dejamos listos los path que usaremos a lo largo del script.
- download_csv: mediante un request al url proporcionado generamos la descarga del csv.
- perform_data_quality_checks: revisamos el csv para encontrar posibles fallos en los datos.
- load_csv_to_sql: generamos la carga de los archivos a la tabla correspondiente.
- remove_duplicate_rows: corroboramos que la tabla no contenga rows duplicadas, en base a las columnas 
ID, MUESTRA y RESULTADO

# Criterios utilizados
 
 Los principales criterios se dieron en base a las condiciones proporcionadas, en el caso del scheduler
en este caso seleccione "Task Schedule" de Windows, ya que es lo mas eficiente a la hora de estar en un
sistema windows, en cambio si tendria otro sistema operativo o quisiera hacerlo en un servido no windows
podria optar por opciones como AirFlow, o Crontab (en el caso linux). Respecto al etl, mi preferencia es
la creacion del mismo mediante Python, ya que genera una amplia compatibilidad con diversos sistemas operativos
o cloud, y el configurarlo para otra maquina seria muy sencillo. 

# Dificultades encontradas

 En cuanto a las dificultades encontradas en el camino, podria nombrar que respecto al csv proporcionado
no se tenia explicaciones de los diversos datos contenidos en el mismo, lo que genera que a la hora de querer
normalizarlos o revisar si podria contener algun tipo de error, deje una incognita. Y por otro lado creo que
las consignas en el test son bastante abiertas, y se pueden tomar de diversas maneras. Igualmente este punto
me gusta, ya que deja ver como actua cada persona.

# Tiempo estimado

Pense que iba a llevarme aproximadamente 8hs, luego al leerlo mas detenidamente y comenzando a realizar los 
diversos, me termino llevando 10hs aproximadamente.

# Sitios Utilizados

 Los sitios utilizados para generar este codigo fueron youtube para ver el uso parcial de algunos metodos, 
stackoverflow para la busqueda de casos similares y la documentacion de las librerias utilizadas para revisar algunos 
metodos de las mismas.
