// Databricks notebook source
// MAGIC %md
// MAGIC #6- Spark

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
/*importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que 
ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y 
que infiera el esquema.*/


val file = "/FileStore/tables/Rango_Edades_Seccion_202204.csv"

val schema = "COD_DISTRITO INT,DESC_DISTRITO STRING, COD_DIST_BARRIO INT, DESC_BARRIO STRING, COD_BARRIO INT, COD_DIST_SECCION INT, COD_SECCION INT, COD_EDAD_INT INT, EspanolesHombres INT, EspanolesMujeres INT, ExtranjerosHombres INT, ExtranjerosMujeres INT"

val df = spark.read.format("csv").schema(schema).option("delimiter",";").option("header","true").option("emptyValue",0).option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false").option("quote", "\"").load(file)

//df.show()
/*
De manera alternativa también se puede importar el csv con menos tratamiento en la 
importación y hacer todas las modificaciones para alcanzar el mismo estado de limpieza de
los datos con funciones de Spark.
*/
val df_1 = df.na.fill(value=0)
.withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
.withColumn("DESC_BARRIO",trim(col("desc_barrio")))

//Enumera todos los barrios diferentes.
display(df.select(countDistinct("desc_barrio")).alias("n_barrios"))
val df2 = df.select("DESC_BARRIO").agg(countDistinct("DESC_BARRIO").alias("Cantidad"))

//Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
//diferentes que hay.
df.createTempView("padron")
spark.sql("SELECT COUNT(DISTINCT(DESC_BARRIO)) FROM padron")

//Crea una nueva columna que muestre la longitud de los campos de la columna 
//DESC_DISTRITO y que se llame "longitud".
val df3 = df.withColumn("longitud",length(col("DESC_DISTRITO")))

//Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
val df4 = df.withColumn("constante",lit(5))

//Borra esta columna.
df4.drop(col("constante"))

//Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO
val df5 = df.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

//Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado
//de los rdds almacenados.
df5.cache()

/* muestre el número total de "espanoleshombres", "espanolesmujeres", "extranjeroshombres" y "extranjerosmujeres" 
para cada barrio de cada distrito.  Las columnas distrito y barrio deben ser las primeras en 
aparecer en el show deben estar ordenados en orden de más a menos 
según la columna "extranjerosmujeres" y desempatarán por la columna 
"extranjeroshombres". */
val df6 = df.groupBy(col("desc_barrio"),col("desc_distrito"))
.agg(sum(col("espanolesHombres")).alias("espanolesHombres"), sum(col("espanolesMujeres")).alias("espanolesMujeres"), sum(col("extranjerosHombres")).alias("extranjerosHombres"), sum(col("extranjerosMujeres")).alias("extranjerosMujeres"))
.orderBy(col("desc_distrito"), desc("extranjerosMujeres"),desc("extranjerosHombres"))

//Elimina el registro en caché.
spark.catalog.clearCache()

/*Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con 
DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" 
residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a 
través de las columnas en común.*/

val df7 = df.groupBy(col("desc_barrio"),col("desc_distrito"))
.agg(sum(col("espanolesHombres")).alias("totalEspanolesHombres"))
.orderBy(col("desc_distrito"))

val join1 = df_1.join(df7,df("desc_distrito") === df7("desc_distrito") && df("desc_barrio")=== df7("desc_barrio"),"inner")
.select(df_1("desc_barrio"), df_1("cod_seccion"),df_1("cod_barrio"),df7("totalEspanolesHombres"))

//Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
val df8 = df.withColumn("totalEspanolesHombres", sum(col("espanoleshombres")).over(Window.partitionBy("DESC_BARRIO","DESC_DISTRITO")))

/*Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y 
en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente 
CENTRO, BARAJAS y RETIRO y deben figurar como columnas . */
val distritos = Seq("CENTRO", "BARAJAS", "RETIRO")
val df9 = df_1.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO", distritos).agg(sum("EspanolesMujeres")).orderBy("COD_EDAD_INT")

/*Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje 
de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa 
cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la 
condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.*/
val df10 = df9.withColumn("%_barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))
.withColumn("%_centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))
.withColumn("%_retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))

/*Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un 
directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba 
que es la esperada.*/
val df5 = df.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
df5.write.format("csv").mode("overwrite").save("/FileStore/tables/Padron")

/*Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el 
resultado anterior.*/
val df5 = df.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
df5.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/FileStore/tables/Padron2")


display(dbutils.fs.ls("/FileStore/tables/Padron"))
display(dbutils.fs.ls("/FileStore/tables/Padron2"))
//las particiones guardadas en formato parquet ocupan menos tamaño que las particiones guardadas en csv

// COMMAND ----------

// MAGIC %md
// MAGIC #7- Spark y Hive
// MAGIC utilizando desde Spark EXCLUSIVAMENTE 
// MAGIC sentencias spark.sql, es decir, importar los archivos desde local directamente como tablas
// MAGIC de Hive y haciendo todas las consultas sobre estas tablas sin transformarlas en ningún 
// MAGIC momento en DataFrames ni DataSets

// COMMAND ----------

// DBTITLE 1,Crear una base de datos 
import org.apache.spark.sql.functions._

spark.sql("create database datos_padron")

// COMMAND ----------

// DBTITLE 1,Crear tabla y cargar los datos 
spark.sql("""USE datos_padron;""")
spark.sql("""
CREATE TABLE IF NOT EXISTS padron_txt USING CSV
OPTIONS (
header="true",
delimiter=";",
inferSchema="true",
path="/FileStore/tables/Rango_Edades_Seccion_202204.csv"
)
""")

// COMMAND ----------

// DBTITLE 1,Eliminar los espacios innecesarios y guardandolo en una nueva tabla
spark.sql("""CREATE TABLE padron_txt_2 AS
SELECT TRIM(COD_DISTRITO) AS COD_DISTRITO, TRIM(DESC_DISTRITO) AS DESC_DISTRITO, TRIM(COD_DIST_BARRIO) AS COD_DIST_BARRIO, TRIM(DESC_BARRIO) AS DESC_BARRIO, TRIM(COD_BARRIO) AS COD_BARRIO, TRIM(COD_DIST_SECCION) AS COD_DIST_SECCION, TRIM(COD_SECCION) AS COD_SECCION, TRIM(COD_EDAD_INT) AS COD_EDAD_INT, TRIM(EspanolesHombres) AS EspanolesHombres, TRIM(EspanolesMujeres) AS EspanolesMujeres, TRIM(ExtranjerosHombres) AS ExtranjerosHombres, TRIM(ExtranjerosMujeres) AS ExtranjerosMujeres
FROM padron_txt;""")

// COMMAND ----------

// DBTITLE 1,Sustituir espacios en blanco por 0 
spark.sql("""CREATE TABLE patron_txt_3 AS
SELECT COD_DISTRITO,DESC_DISTRITO, COD_DIST_BARRIO,DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION,
CASE
when LENGTH(EspanolesHombres) > 0 then EspanolesHombres ELSE 0 END AS EspanolesHombres,
CASE
when LENGTH(EspanolesMujeres) > 0 then EspanolesMujeres ELSE 0 END AS EspanolesMujeres,
CASE
when LENGTH(ExtranjerosHombres) > 0 then ExtranjerosHombres ELSE 0 END AS ExtranjerosHombres,
CASE
when LENGTH(ExtranjerosMujeres) > 0 then ExtranjerosMujeres ELSE 0 END AS ExtranjerosMujeres
from padron_txt;""")

// COMMAND ----------

// DBTITLE 1,Regex
spark.sql("""CREATE TABLE padron_regex (COD_DISTRITO INT,DESC_DISTRITO STRING,
COD_DIST_BARRIO INT, DESC_BARRIO STRING,
COD_BARRIO INT,COD_DIST_SECCION INT,COD_SECCION INT,COD_EDAD_INT INT, EspanolesHombres INT,EspanolesMujeres INT,ExtranjerosHombres INT,ExtranjerosMujeres INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ("input.regex"='"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"(.*?)"\073"(.*?)"\073"(.*?)"') STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");""")

// COMMAND ----------

spark.sql("""LOAD DATA INPATH '/FileStore/tables/Rango_Edades_Seccion_202204.csv' INTO TABLE padron_regex""")

// COMMAND ----------

spark.sql("""CREATE TABLE padron_parquet
STORED AS PARQUET
AS
SELECT * FROM padron_txt_2;""")