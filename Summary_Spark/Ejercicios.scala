// Databricks notebook source
// MAGIC %md
// MAGIC #Extras del tema 2

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("Quijote").getOrCreate()
val file = spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/tables/el_quijote.txt")

//cuenta el numero de filas
//file.count()

//muestra por defecto las primeras 20 filas
//file.show()

//muestra las 10 primeras filas como una tabla, por defecto es true por lo que se trunca el texto
//file.show(10)

//igual que el anterior, truncado false por lo que se muestra todo
//file.show(10,false)

//muestra las 2 primeras filas
//file.head()

//muestra las 3 primeras filas, mostrandolo como un array de string
//file.head(2)

//muestra las primeras 5 filas pero mostrandolo como un array
//file.take(5)

//muestra la primera fila
//file.first()

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

val spark = SparkSession.builder.appName("MnMCount").getOrCreate()

val mnmDF = spark.read
.option("header","true")
.option("inferSchema","true")
.csv("/FileStore/tables/mnm_dataset.csv")

//selecciona la cantidad maxima individual y la muestra
//val countMnMDF=mnmDF.select(F.max("Count")).show()

//la funcion agg() permite a los DF hacer funciones de max(),min(),etc

/*
//agrupa por provincia y color, despues se hace una suma y por ultimo se ordena descendentemente
val countMnMDF=mnmDF
.select("State","Color","Count")
.groupBy("State","Color")
.agg(count("Count").alias("Total"))
.orderBy(desc("Total"))
//se selecciona la cantidad maxima del resultado anterior y se muestra
val MnMmax = countMnMDF.select(F.max("Total")).show()
*/

/*
//igual que la anterior pero selecciona los estados de CA y el de NV
val caCountMnMDF = mnmDF
 .select("State", "Color", "Count")
 .where(col("State") === "CA" || col("State") === "NV")
 .groupBy("State", "Color")
 .agg(count("Count").alias("Total"))
 .orderBy(desc("Total")).show()
 */

//varias operaciones
//val funciones = mnmDF.select(F.sum("Count"), F.avg("Count"),F.min("Count"), F.max("Count")).show()



// COMMAND ----------

// MAGIC %md
// MAGIC #Tema 3

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql.functions import *
// MAGIC # Programmatic way to define a schema 
// MAGIC fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
// MAGIC                           StructField('UnitID', StringType(), True),
// MAGIC                           StructField('IncidentNumber', IntegerType(), True),
// MAGIC                           StructField('CallType', StringType(), True), 
// MAGIC                           StructField('CallDate', StringType(), True), 
// MAGIC                           StructField('WatchDate', StringType(), True),
// MAGIC                           StructField('CallFinalDisposition', StringType(), True),
// MAGIC                           StructField('AvailableDtTm', StringType(), True),
// MAGIC                           StructField('Address', StringType(), True), 
// MAGIC                           StructField('City', StringType(), True), 
// MAGIC                           StructField('Zipcode', IntegerType(), True), 
// MAGIC                           StructField('Battalion', StringType(), True), 
// MAGIC                           StructField('StationArea', StringType(), True), 
// MAGIC                           StructField('Box', StringType(), True), 
// MAGIC                           StructField('OriginalPriority', StringType(), True), 
// MAGIC                           StructField('Priority', StringType(), True), 
// MAGIC                           StructField('FinalPriority', IntegerType(), True), 
// MAGIC                           StructField('ALSUnit', BooleanType(), True), 
// MAGIC                           StructField('CallTypeGroup', StringType(), True),
// MAGIC                           StructField('NumAlarms', IntegerType(), True),
// MAGIC                           StructField('UnitType', StringType(), True),
// MAGIC                           StructField('UnitSequenceInCallDispatch', IntegerType(), True),
// MAGIC                           StructField('FirePreventionDistrict', StringType(), True),
// MAGIC                           StructField('SupervisorDistrict', StringType(), True),
// MAGIC                           StructField('Neighborhood', StringType(), True),
// MAGIC                           StructField('Location', StringType(), True),
// MAGIC                           StructField('RowID', StringType(), True),
// MAGIC                           StructField('Delay', FloatType(), True)])
// MAGIC # Use the DataFrameReader interface to read a CSV file
// MAGIC sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
// MAGIC fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
// MAGIC 
// MAGIC fire_ts_df = (fire_df
// MAGIC  .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
// MAGIC  .drop("CallDate")
// MAGIC  .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
// MAGIC  .drop("WatchDate")
// MAGIC  .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
// MAGIC  "MM/dd/yyyy hh:mm:ss a"))
// MAGIC  .drop("AvailableDtTm"))
// MAGIC 
// MAGIC #distintos tipos de llamadas en 2018 / What were all the different types of fire calls in 2018?
// MAGIC #display(fire_ts_df
// MAGIC #.select("CallType")
// MAGIC #.where(year('IncidentDate') == "2018")
// MAGIC #.distinct())
// MAGIC 
// MAGIC #numero de llamadas agrupadas por mes en el año 2018 / What months within the year 2018 saw the highest number of fire calls?
// MAGIC #display(fire_ts_df
// MAGIC #.select('IncidentDate')
// MAGIC #.where(year('IncidentDate') == "2018")
// MAGIC #.orderBy(month('IncidentDate'), ascending=False)
// MAGIC #.groupBy(month('IncidentDate'))
// MAGIC #.count())
// MAGIC 
// MAGIC #numero de llamadas por vencidario en san francisco / Which neighborhood in San Francisco generated the most fire calls in 2018?
// MAGIC #display(fire_ts_df.select('Neighborhood').where((col('City') == 'San Francisco') & (year('IncidentDate') == "2018")).groupBy('Neighborhood').count())
// MAGIC 
// MAGIC # los neighborhoods del 2018 ordenados por mayor tiempo de respuesta / Which neighborhoods had the worst response times to fire calls in 2018? AvailableDtTS
// MAGIC #display(fire_ts_df.select('Neighborhood','AvailableDtTS').where(year('IncidentDate') == "2018").orderBy('AvailableDtTS', ascending=False))
// MAGIC 
// MAGIC # agrupado por semanas del año de 2018 / Which week in the year in 2018 had the most fire calls?
// MAGIC #fire_ts_df.filter(year("IncidentDate")==2018).groupBy(weekofyear("IncidentDate")).count().orderBy("count",ascending=False).show()
// MAGIC #display(fire_ts_df.select('IncidentDate').where(year('IncidentDate') == "2018").groupBy(weekofyear('IncidentDate')).count())
// MAGIC 
// MAGIC #How can we use Parquet files or SQL tables to store this data and read it back?
// MAGIC #parquet_path = "/FileStore/tables/prueba2.txt"
// MAGIC #fire_df.write.format("parquet").save(parquet_path)

// COMMAND ----------

case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
val ds = spark.read
.json("/FileStore/tables/iot_devices.json")
.as[DeviceIoTData]

// Detect failing devices with battery levels below a threshold.
//val filtro = ds.filter(d => d.battery_level < 1)
//filtro.show()

// Identify offending countries with high levels of CO2 emissions.
//val filtro = ds.filter(d => d.c02_level > 1400)
//filtro.show()

//Compute the min and max values for temperature, battery level, CO2, and humidity
/*
val windowSpec2  = Window.partitionBy("c02_level")
val windowSpec  = Window.partitionBy("temp")
val aggDF = ds.withColumn("max", max(col("temp")).over(windowSpec)).withColumn("max", max(col("c02_level")).over(windowSpec2)).select("temp","c02_level")
aggDF.show()
*/ 
//val funciones = ds.select(F.min("temp"), F.max("temp"),F.min("c02_level"), F.max("c02_level"),F.min("battery_level"), F.max("battery_level"),F.min("humidity"), //F.max("humidity")).show()
//Sort and group by average temperature, CO2, humidity, and country
//ds.groupBy("cn").avg("temp","c02_level","humidity").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #Extras del tema 3

// COMMAND ----------

//Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.
val mnmDF = spark.read
.option("header","true")
.option("inferSchema","true")
.csv("/FileStore/tables/mnm_dataset.csv")
//mnmDF.printSchema()

// Cuando se define un schema al definir un campo por ejemplo StructField('Delay', FloatType(), True) ¿qué significa el último parámetro Boolean?
//si admite o no nulos

//Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?
//la diferencia es que el dataframe puede contener caracteres alfanumericos 

//Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y guardar los datos en los formatos: JSON, CSV, AVRO
/*val json_path = "/FileStore/tables/ejercicioExtra1.json"
mnmDF.write.format("JSON").save(json_path)
val csv_path = "/FileStore/tables/ejercicioExtra2.csv"
mnmDF.write.format("CSV").save(csv_path)
val avro_path = "/FileStore/tables/ejercicioExtra3.avro"
mnmDF.write.format("AVRO").save(avro_path)*/

//¿A qué se debe que hayan más de un fichero?
//se debe a que se divide en tantos archivos como particiones haya 
//¿Cómo obtener el número de particiones de un DataFrame?
//mnmDF.rdd.getNumPartitions
//mnmDF.rdd.partitions.size
//¿Qué formas existen para modificar el número de particiones de un DataFrame?
//val new_mnmDF = mnmDF.repartition(5)
//val new2_mnmDF = new_mnmDF.coalesce(3)
//new2_mnmDF.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC #Tema 4

// COMMAND ----------

val csvFile="/FileStore/tables/departuredelays.csv"
val df = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csvFile)
df.createOrReplaceTempView("us_delay_flights_tbl")
// convert the date column into a readable format and find the days or months when these delays were most common
spark.sql("""SELECT cast(date to Timestamp), delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

//Were the delays related to winter months or holidays?


// COMMAND ----------

// MAGIC %python
// MAGIC csv_file = "/FileStore/tables/departuredelays.csv"
// MAGIC # Read and create a temporary view
// MAGIC # Infer schema (note that for larger files you 
// MAGIC # may want to specify the schema)
// MAGIC df = (spark.read.format("csv")
// MAGIC  .option("inferSchema", "true")
// MAGIC  .option("header", "true")
// MAGIC  .load(csv_file))
// MAGIC df.createOrReplaceTempView("us_delay_flights_tbl")
// MAGIC from pyspark.sql.functions import col, desc,when, asc
// MAGIC 
// MAGIC #try converting the other two SQL queries to use the DataFrame API
// MAGIC #first
// MAGIC #(df.select("date", "delay", "origin", "destination")
// MAGIC  #.where((col("delay") > 120) & (col("origin") == 'SFO') & (col("destination") == 'ORD'))
// MAGIC  #.orderBy("delay", ascending=False).show(10))
// MAGIC #second
// MAGIC df.select(col("delay"),col("origin"),col("destination"),when(df.delay > 360 , "Very Long Delays")
// MAGIC .when((df.delay >120) & (df.delay < 360), "Long Delays")
// MAGIC .when((df.delay >60) & (df.delay < 120), "Short Delays")
// MAGIC .when((df.delay >0) & (df.delay < 60), "Tolerable Delays")
// MAGIC .otherwise("Early").alias("Flight_Delays")).orderBy("origin","delay",ascending=False).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #Extras del tema 4

// COMMAND ----------

//GlobalTempView vs TempView 
//las vistas globales son accesibles por todas las sesiones que esten iniciadas y se pueden utilizar para compartir informacion, sin embargo las vistas globales solo estan dispobles en la sesion en la que se haya iniciado 

//Leer los AVRO, Parquet, JSON y CSV escritos en el cap3

val file = """/FileStore/tables/ejercicioExtra1.json"""
val df = spark.read.format("json").load(file)

/*val file = "/FileStore/tables/ejercicioExtra2.csv"
val df = spark.read.format("csv")
 .option("header", "true")
 .option("mode", "FAILFAST") 
 .option("nullValue", "")
 .load(file)*/

/*val df = spark.read.format("avro")
.load("/FileStore/tables/ejercicioExtra3.avro")
df.show(false)*/

//val file = """/FileStore/tables/ejercicioExtra.parquet"""
//val df = spark.read.format("parquet").load(file)
