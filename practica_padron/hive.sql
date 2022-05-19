-- Databricks notebook source
-- MAGIC %md
-- MAGIC #1- Creación de tablas en formato texto.

-- COMMAND ----------

-- DBTITLE 1,Crear Base de datos "datos_padron"
CREATE DATABASE datos_padron

-- COMMAND ----------

USE datos_padron

-- COMMAND ----------

-- DBTITLE 1,Crear la tabla de datos
CREATE TABLE padron_txt (COD_DISTRITO INT,DESC_DISTRITO STRING,
COD_DIST_BARRIO INT, DESC_BARRIO STRING,
COD_BARRIO INT,COD_DIST_SECCION INT,COD_SECCION INT,COD_EDAD_INT INT, EspanolesHombres INT,EspanolesMujeres INT,ExtranjerosHombres INT,ExtranjerosMujeres INT) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = '\073',
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Se suben los datos a hadoop y se visualizan para comrpobar que se ha hecho correctamente
-- MAGIC 
-- MAGIC - hadoop fs -put Rango_Edades_Seccion_202204.csv /user/cloudera/hive/
-- MAGIC 
-- MAGIC - hadoop fs -ls -R

-- COMMAND ----------

-- DBTITLE 1,Cargar datos
LOAD DATA INPATH '/user/cloudera/hive/Rango_Edades_Seccion_202204.csv' INTO TABLE datos_padron.padron_txt

-- COMMAND ----------

-- DBTITLE 1,eliminar los espacios innecesarios y guardandolo en una nueva tabla
CREATE TABLE padron_txt_2 AS
SELECT TRIM(COD_DISTRITO) AS COD_DISTRITO, TRIM(DESC_DISTRITO) AS DESC_DISTRITO, TRIM(COD_DIST_BARRIO) AS COD_DIST_BARRIO, TRIM(DESC_BARRIO) AS DESC_BARRIO, TRIM(COD_BARRIO) AS COD_BARRIO, TRIM(COD_DIST_SECCION) AS COD_DIST_SECCION, TRIM(COD_SECCION) AS COD_SECCION, TRIM(COD_EDAD_INT) AS COD_EDAD_INT, TRIM(EspanolesHombres) AS EspanolesHombres, TRIM(EspanolesMujeres) AS EspanolesMujeres, TRIM(ExtranjerosHombres) AS ExtranjerosHombres, TRIM(ExtranjerosMujeres) AS ExtranjerosMujeres
FROM padron_txt;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Diferencia de incluir la palabra LOCAL en el comando LOAD DATA:
-- MAGIC 
-- MAGIC Con la palabra local se buscan en el sistema de archivos local, mientras que si se omite se busca el archivo en HDFS

-- COMMAND ----------

-- DBTITLE 1,Sustituir espacios en blanco por 0 
CREATE TABLE patron_txt_3 AS
SELECT COD_DISTRITO,DESC_DISTRITO, COD_DIST_BARRIO,DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION,
CASE
when LENGTH(EspanolesHombres) > 0 then EspanolesHombres ELSE 0 END AS EspanolesHombres,
CASE
when LENGTH(EspanolesMujeres) > 0 then EspanolesMujeres ELSE 0 END AS EspanolesMujeres,
CASE
when LENGTH(ExtranjerosHombres) > 0 then ExtranjerosHombres ELSE 0 END AS ExtranjerosHombres,
CASE
when LENGTH(ExtranjerosMujeres) > 0 then ExtranjerosMujeres ELSE 0 END AS ExtranjerosMujeres
from padron_txt

-- COMMAND ----------

-- DBTITLE 1,Regex
CREATE TABLE padron_txt (COD_DISTRITO INT,DESC_DISTRITO STRING,
COD_DIST_BARRIO INT, DESC_BARRIO STRING,
COD_BARRIO INT,COD_DIST_SECCION INT,COD_SECCION INT,COD_EDAD_INT INT, EspanolesHombres INT,EspanolesMujeres INT,ExtranjerosHombres INT,ExtranjerosMujeres INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ("input.regex"='"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"(.*?)"\073"(.*?)"\073"(.*?)"') STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2- Formato columnar parquet.
-- MAGIC 
-- MAGIC ¿Qué es CTAS?
-- MAGIC - es una funcion de SQL que sirve para crear una nueva tabla e insertar datos procedentes de otra tabla de forma rapida 
-- MAGIC 
-- MAGIC CTAS tiene estas restricciones:
-- MAGIC 
-- MAGIC - La tabla de destino no puede ser una tabla particionada.
-- MAGIC - La tabla de destino no puede ser una tabla externa.
-- MAGIC - La tabla de destino no puede ser una tabla de agrupación de listas.

-- COMMAND ----------

-- DBTITLE 1,Crear tabla Hive padron_parquet
CREATE TABLE padron_parquet
STORED AS PARQUET
AS
SELECT * FROM padron_txt;

-- COMMAND ----------

CREATE TABLE padron_parquet_2
STORED AS PARQUET
AS
SELECT * FROM padron_txt_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar 
-- MAGIC con este tipo de formatos.
-- MAGIC 
-- MAGIC Parquet es el formato de almacenamiento en columnas principal en el ecosistema Hadoop. Parquet es un formato de almacenamiento columnar que admite estructuras anidadas. Muy adecuado para escenarios OLAP, almacenamiento de columnas y escaneo
-- MAGIC 
-- MAGIC Ventajas:
-- MAGIC - El almacenamiento de columnas facilita el uso de una codificación y compresión eficientes para cada columna, lo que reduce el espacio en disco (comprension mas alta).
-- MAGIC -  Operaciones IO más pequeñas. Utiliza la inserción de mapas y la inserción de predicados para leer solo las columnas requeridas y omitir las columnas que no cumplan con las condiciones, lo que puede reducir el escaneo de datos innecesario, traer mejoras de rendimiento y volverse más obvias cuando hay más campos de tabla

-- COMMAND ----------

-- MAGIC %md
-- MAGIC comprobar el tamaño de las tablas de hadoop
-- MAGIC 
-- MAGIC padron_parquet
-- MAGIC - rawDataSize 	2.73 MB
-- MAGIC - totalSize 	912.75 KB
-- MAGIC 
-- MAGIC padron_parquet_2
-- MAGIC - rawDataSize 	2.73 MB
-- MAGIC - totalSize 	910.76 KB
-- MAGIC 
-- MAGIC padron_txt
-- MAGIC - totalSize 	21.59 MB
-- MAGIC 
-- MAGIC padron_txt_2
-- MAGIC - rawDataSize 	11.39 MB
-- MAGIC - totalSize 	11.62 MB

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3- Impala
-- MAGIC 
-- MAGIC ¿Que es Impala?
-- MAGIC 
-- MAGIC Apache Impala es una herramienta escalable de procesamiento MPP (Massively Parallel Processing). Realiza consultas SQL interactivas con muy baja latencia. Además, soporta múltiples formatos como Parquet, ORC, JSON o Avro y tecnologías de almacenamiento como HDFS, Kudu, Hive, HBase, Amazon S3 o ADLS.
-- MAGIC 
-- MAGIC Impala usa los mismos metadatos, la misma sintaxis SQL y el mismo driver que Hive. Pero Impala destaca cuando necesitamos una tecnología que nos proporcione una baja latencia en consultas exploratorias y de descubrimiento de datos.
-- MAGIC 
-- MAGIC ¿En qué se diferencia de Hive?
-- MAGIC 
-- MAGIC - En Hive, la latencia es alta, pero en Impala, la latencia es baja .
-- MAGIC - Hive admite el formato de archivo de formato de columna optimizada (ORC) con compresión Zlib, pero Impala admite el formato Parquet con compresión rápida .
-- MAGIC - Hive está escrito en Java pero Impala está escrito en C ++.
-- MAGIC - Hive no admite el procesamiento en paralelo, pero Impala admite el procesamiento en paralelo.
-- MAGIC - Hive admite MapReduce pero Impala no es compatible con MapReduce.
-- MAGIC - Hive es tolerante a fallas, pero Impala no admite tolerancia a fallas.
-- MAGIC - Hive admite tipos complejos, pero Impala no admite tipos complejos .
-- MAGIC - Hive es Hadoop MapReduce basado en lotes, pero Impala es una base de datos MPP .
-- MAGIC - Hive no es compatible con la informática interactiva, pero Impala es compatible con la informática interactiva .
-- MAGIC - El público de hive son ingenieros de datos, pero el público de Impala son analistas de datos / científicos de datos.
-- MAGIC 
-- MAGIC Comando INVALIDATE METADATA, ¿en qué consiste?
-- MAGIC 
-- MAGIC Declara los metadatos de una o todas las tablas como obsoletas. La próxima vez que el servicio de Impala realice una consulta en una tabla cuyos metadatos estén invalidados, Impala recargará los metadatos asociados antes de continuar con la consulta. Como esta es una operación muy costosa en comparación con la actualización incremental de metadatos realizada por REFRESH, cuando sea posible, prefiera REFRESH en lugar de INVALIDATE METADATA.

-- COMMAND ----------

-- DBTITLE 1,INVALIDATE METADATA
INVALIDATE METADATA padron_txt

-- COMMAND ----------

-- DBTITLE 1,total de ciudadanos agrupados por DESC_DISTRITO y DESC_BARRIO
SELECT desc_distrito,
       desc_barrio,
       sum(cast(espanoleshombres AS INT)),
       sum(cast(espanolesmujeres AS INT)),
       sum(cast(extranjeroshombres AS INT)),
       sum(cast(extranjerosmujeres AS INT))
FROM padron_txt_2
GROUP BY desc_distrito,
         desc_barrio
ORDER BY desc_distrito

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2.  Llevar a cabo la misma consulta sobre las mismas tablas en Impala. ¿Alguna 
-- MAGIC conclusión?
-- MAGIC  
-- MAGIC  Esa consulta en impala tarda 1.2s y en hive tarda 1m 7s
-- MAGIC  
-- MAGIC  Mientras que en parquet con hive tarda 56.20s
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #4- Tablas particionadas.

-- COMMAND ----------

-- DBTITLE 1,Crear tabla e insertar de forma dinámica
CREATE TABLE padron_particionado (COD_DISTRITO string,  
COD_DIST_BARRIO string, COD_BARRIO string,COD_DIST_SECCION string,COD_SECCION string,
COD_EDAD_INT string, EspanolesHombres string,EspanolesMujeres string,ExtranjerosHombres string,ExtranjerosMujeres string) PARTITIONED BY (desc_distrito string,  
desc_barrio string) STORED AS PARQUET;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET mapreduce.map.memory.mb = 2048;
SET mapreduce.reduce.memory.mb = 2048;
SET mapreduce.map.java.opts=-Xmx1800m;

INSERT OVERWRITE TABLE padron_particionado 
PARTITION(desc_distrito,desc_barrio) 
SELECT COD_DISTRITO, 
COD_DIST_BARRIO, COD_BARRIO,COD_DIST_SECCION, COD_SECCION, 
COD_EDAD_INT, EspanolesHombres,EspanolesMujeres, 
ExtranjerosHombres, ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO 
FROM padron_parquet_2;

-- COMMAND ----------

INSERT OVERWRITE TABLE padron_particionado 
PARTITION(desc_distrito,desc_barrio) 
SELECT COD_DISTRITO, 
COD_DIST_BARRIO, COD_BARRIO,COD_DIST_SECCION, COD_SECCION, 
EspanolesHombres,EspanolesMujeres, 
ExtranjerosHombres, ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO 
FROM padron_parquet_2;

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,Insertar de forma manual 
INSERT INTO padron_particionado PARTITION(desc_distrito, desc_barrio) 
SELECT COD_DISTRITO,  
COD_DIST_BARRIO, COD_BARRIO,COD_DIST_SECCION, COD_SECCION,  
COD_EDAD_INT, EspanolesHombres,EspanolesMujeres,  
ExtranjerosHombres, ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO 
FROM padron_parquet_2 
WHERE desc_distrito = 'CHAMARTIN' 
AND desc_barrio = 'CASTILLA';

-- COMMAND ----------

-- DBTITLE 1,Total por distritos
SELECT desc_distrito,
       desc_barrio,
       sum(CAST(EspanolesHombres AS INT))AS EspanolesHombres,
       sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
       sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
       sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
FROM padron_particionado
WHERE desc_distrito = "CENTRO"
  OR desc_distrito="LATINA"
  OR desc_distrito="CHAMARTIN"
  OR desc_distrito="TETUAN"
  OR desc_distrito="VICALVARO"
  OR desc_distrito="BARAJAS"
GROUP BY desc_distrito,
         desc_barrio;

/*
Llevar a cabo la consulta en Hive en las tablas padron_parquet y 
padron_partitionado. ¿Alguna conclusión?

la tabla original tarda mas en mostrar los datos que en la particionada (25s / 32s)

Llevar a cabo la consulta en Impala en las tablas padron_parquet y 
padron_particionado. ¿Alguna conclusión?

en Impala la particionada tarda 0s y la orginal tarda 2s
*/

-- COMMAND ----------

-- DBTITLE 1,consultas de agregación (Max, Min, Avg, Count)
SELECT desc_distrito,
       desc_barrio,
       avg(CAST(EspanolesHombres AS INT)) AS media,
       max(CAST(EspanolesHombres AS INT)) AS maximo,
       min(CAST(EspanolesHombres AS INT)) AS minimo,
       count(CAST(EspanolesHombres AS INT)) AS cantidad
FROM padron_particionado
WHERE desc_distrito IN ('CENTRO',
                        'LATINA',
                        'CHAMARTIN',
                        'TETUAN',
                        'VICALVARO',
                        'BARAJAS')
GROUP BY desc_distrito,
         desc_barrio;
         
 /*
 con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y 
comparar rendimientos tanto en Hive como en Impala y sacar conclusiones
La tabla particionada en hive tarda 25s, mientras que en Impala tarda 1s.
Por otra parte las tablas parquet y txt_2 tardan 27s y 32s respectivamente
 */

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #5- Tablas en HDFS
-- MAGIC 
-- MAGIC Crear un directorio en HDFS
-- MAGIC - hdfs dfs -mkdir /user/test
-- MAGIC - hdfs dfs -ls /user/test
-- MAGIC 
-- MAGIC Mueve tu fichero datos1 al directorio que has creado en HDFS con un comando 
-- MAGIC desde consola.
-- MAGIC - hdfs dfs -put /home/cloudera/ejercicios/datos1.txt /user/test/

-- COMMAND ----------

-- DBTITLE 1,Crear la base de datos y tabla, carga los datos
CREATE DATABASE numeros;

CREATE TABLE numeros_tbl (col1 INT, col2 INT, col3 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/user/test/datos1.txt' INTO TABLE numeros_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Consulta la localización donde estaban anteriormente los datos 
-- MAGIC almacenados. ¿Siguen estando ahí? ¿Dónde están?
-- MAGIC - no, los datos se encuentran en /user/hive/warehouse/numeros.db/numeros_tbl/datos1.txt

-- COMMAND ----------

-- DBTITLE 1,Borrar la tabla
DROP TABLE numeros_tbl 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Al borrar la tabla, ¿qué ocurre con los datos almacenados en HDFS?
-- MAGIC - Al borrar la tabla interna tambien se borran los metadatos y los datos almacenado en HDFS 

-- COMMAND ----------

-- DBTITLE 1,Crear una tabla externa y cargar datos
CREATE EXTERNAL TABLE numeros_tbl (col1 INT, col2 INT, col3 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/user/test/datos1.txt' INTO TABLE numeros.numeros_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ¿A dónde han ido los datos en HDFS? 
-- MAGIC - Los datos no estan en la carpeta inicial, ahora se encuentran en /user/hive/warehouse/numeros.db/numeros_tbl
-- MAGIC 
-- MAGIC Al borrar la tabla ¿Qué ocurre con los datos en hdfs?
-- MAGIC - Los datos siguen estando en la carpeta donde se encontraban
-- MAGIC 
-- MAGIC Borra el fichero datos1 del directorio en el que estén
-- MAGIC -  hdfs dfs -rm -r /user/hive/warehouse/numeros.db/numeros_tbl/*
-- MAGIC 
-- MAGIC Vuelve a insertarlos en el directorio que creamos inicialmente (/test)
-- MAGIC - hdfs dfs -put /home/cloudera/ejercicios/datos1.txt /user/test/

-- COMMAND ----------

-- DBTITLE 1,Crear la tabla números de manera externa y con un argumento location
CREATE EXTERNAL TABLE numeros_tbl (col1 INT, col2 INT, col3 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/home/cloudera/ejercicios/';
/* Si da error, utilizar esta forma alternativa */
CREATE EXTERNAL TABLE numeros_tbl (col1 INT, col2 INT, col3 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
alter table numeros_tbl set location '/user/test/';

/*
¿Tiene algún contenido?
Si, ya que los datos se han importado automaticamente

Inserta el fichero de datos creado al principio, "datos2" en el mismo directorio de 
HDFS que "datos1". Vuelve a hacer la consulta anterior sobre la misma tabla. ¿Qué salida muestra?
Muestra todos los datos que hay guardados en ese directorio, como datos1.txt como datos2.txt
*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #6- Spark

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC import org.apache.spark.sql.expressions.Window
-- MAGIC /*importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que 
-- MAGIC ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y 
-- MAGIC que infiera el esquema.*/
-- MAGIC 
-- MAGIC 
-- MAGIC val file = "/FileStore/tables/Rango_Edades_Seccion_202204.csv"
-- MAGIC 
-- MAGIC val schema = "COD_DISTRITO INT,DESC_DISTRITO STRING, COD_DIST_BARRIO INT, DESC_BARRIO STRING, COD_BARRIO INT, COD_DIST_SECCION INT, COD_SECCION INT, COD_EDAD_INT INT, EspanolesHombres INT, EspanolesMujeres INT, ExtranjerosHombres INT, ExtranjerosMujeres INT"
-- MAGIC 
-- MAGIC val df = spark.read.format("csv").schema(schema).option("delimiter",";").option("header","true").option("emptyValue",0).option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false").option("quote", "\"").load(file)
-- MAGIC 
-- MAGIC //df.show()
-- MAGIC /*
-- MAGIC De manera alternativa también se puede importar el csv con menos tratamiento en la 
-- MAGIC importación y hacer todas las modificaciones para alcanzar el mismo estado de limpieza de
-- MAGIC los datos con funciones de Spark.
-- MAGIC */
-- MAGIC val df_1 = df.na.fill(value=0)
-- MAGIC .withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
-- MAGIC .withColumn("DESC_BARRIO",trim(col("desc_barrio")))
-- MAGIC 
-- MAGIC //Enumera todos los barrios diferentes.
-- MAGIC display(df.select(countDistinct("desc_barrio")).alias("n_barrios"))
-- MAGIC val df2 = df.select("DESC_BARRIO").agg(countDistinct("DESC_BARRIO").alias("Cantidad"))
-- MAGIC 
-- MAGIC //Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
-- MAGIC //diferentes que hay.
-- MAGIC df.createTempView("padron")
-- MAGIC spark.sql("SELECT COUNT(DISTINCT(DESC_BARRIO)) FROM padron")
-- MAGIC 
-- MAGIC //Crea una nueva columna que muestre la longitud de los campos de la columna 
-- MAGIC //DESC_DISTRITO y que se llame "longitud".
-- MAGIC val df3 = df.withColumn("longitud",length(col("DESC_DISTRITO")))
-- MAGIC 
-- MAGIC //Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
-- MAGIC val df4 = df.withColumn("constante",lit(5))
-- MAGIC 
-- MAGIC //Borra esta columna.
-- MAGIC df4.drop(col("constante"))
-- MAGIC 
-- MAGIC //Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO
-- MAGIC val df5 = df.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
-- MAGIC 
-- MAGIC //Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado
-- MAGIC //de los rdds almacenados.
-- MAGIC df5.cache()
-- MAGIC 
-- MAGIC /* muestre el número total de "espanoleshombres", "espanolesmujeres", "extranjeroshombres" y "extranjerosmujeres" 
-- MAGIC para cada barrio de cada distrito.  Las columnas distrito y barrio deben ser las primeras en 
-- MAGIC aparecer en el show deben estar ordenados en orden de más a menos 
-- MAGIC según la columna "extranjerosmujeres" y desempatarán por la columna 
-- MAGIC "extranjeroshombres". */
-- MAGIC val df6 = df.groupBy(col("desc_barrio"),col("desc_distrito"))
-- MAGIC .agg(sum(col("espanolesHombres")).alias("espanolesHombres"), sum(col("espanolesMujeres")).alias("espanolesMujeres"), sum(col("extranjerosHombres")).alias("extranjerosHombres"), sum(col("extranjerosMujeres")).alias("extranjerosMujeres"))
-- MAGIC .orderBy(col("desc_distrito"), desc("extranjerosMujeres"),desc("extranjerosHombres"))
-- MAGIC 
-- MAGIC //Elimina el registro en caché.
-- MAGIC spark.catalog.clearCache()
-- MAGIC 
-- MAGIC /*Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con 
-- MAGIC DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" 
-- MAGIC residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a 
-- MAGIC través de las columnas en común.*/
-- MAGIC 
-- MAGIC val df7 = df.groupBy(col("desc_barrio"),col("desc_distrito"))
-- MAGIC .agg(sum(col("espanolesHombres")).alias("totalEspanolesHombres"))
-- MAGIC .orderBy(col("desc_distrito"))
-- MAGIC 
-- MAGIC val join1 = df_1.join(df7,df("desc_distrito") === df7("desc_distrito") && df("desc_barrio")=== df7("desc_barrio"),"inner")
-- MAGIC .select(df_1("desc_barrio"), df_1("cod_seccion"),df_1("cod_barrio"),df7("totalEspanolesHombres"))
-- MAGIC 
-- MAGIC //Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
-- MAGIC val df8 = df.withColumn("totalEspanolesHombres", sum(col("espanoleshombres")).over(Window.partitionBy("DESC_BARRIO","DESC_DISTRITO")))
-- MAGIC 
-- MAGIC /*Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
-- MAGIC contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y 
-- MAGIC en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente 
-- MAGIC CENTRO, BARAJAS y RETIRO y deben figurar como columnas . */
-- MAGIC val distritos = Seq("CENTRO", "BARAJAS", "RETIRO")
-- MAGIC val df9 = df_1.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO", distritos).agg(sum("EspanolesMujeres")).orderBy("COD_EDAD_INT")
-- MAGIC 
-- MAGIC /*Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje 
-- MAGIC de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa 
-- MAGIC cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la 
-- MAGIC condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.*/
-- MAGIC val df10 = df9.withColumn("%_barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))
-- MAGIC .withColumn("%_centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))
-- MAGIC .withColumn("%_retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))
-- MAGIC 
-- MAGIC /*Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un 
-- MAGIC directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba 
-- MAGIC que es la esperada.*/
-- MAGIC val df5 = df.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
-- MAGIC df5.write.format("csv").mode("overwrite").save("/FileStore/tables/Padron")
-- MAGIC 
-- MAGIC /*Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el 
-- MAGIC resultado anterior.*/
-- MAGIC val df5 = df.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
-- MAGIC df5.write.format("parquet")
-- MAGIC  .mode("overwrite")
-- MAGIC  .option("compression", "snappy")
-- MAGIC  .save("/FileStore/tables/Padron2")
