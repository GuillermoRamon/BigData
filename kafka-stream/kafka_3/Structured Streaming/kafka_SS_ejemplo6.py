# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 6
# MAGIC jupyter notebook Ejemplo\ 6\ -\ Spark\ Structured\ Streaming.ipynb

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.functions import window
from pyspark.sql.types import StructType

# COMMAND ----------

spark = SparkSession\
        .builder\
        .appName("StructuredCSVFile")\
        .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC mkdir monitoring_data

# COMMAND ----------

windowSize = "60"
slideSize = "10"

windowDuration = '{} seconds'.format(windowSize)
slideDuration = '{} seconds'.format(slideSize)
monitoring_dir = 'monitoring_data'

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("InteractionCount")\
    .config("spark.eventLog.enabled","true")\
    .config("spark.eventLog.dir","applicationHistory")\
    .master("local[*]")\
    .getOrCreate()

# COMMAND ----------

userSchema = StructType().add("userA","string")\
                            .add("userB","string")\
                            .add("timestamp","timestamp")\
                            .add("interaction","string")

# COMMAND ----------

twitterIDSchema = StructType().add("userA","string")
twitterIDs = spark.read.schema(twitterIDSchema).csv('twitterIDs.csv')
csvDF = spark\
    .readStream\
    .schema(userSchema)\
    .csv(monitoring_dir)

joinedDF = csvDF.join(twitterIDs,"userA")

# COMMAND ----------

interactions = joinedDF.select(joinedDF['userA'],joinedDF['interaction'],joinedDF['timestamp'])

# COMMAND ----------

windowedCounts = interactions\
                .groupBy(window(interactions.timestamp, windowDuration, slideDuration),interactions.userA)\
                .count()

# COMMAND ----------

query = windowedCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .option('truncate','false')\
    .option('numRows','10000')\
    .trigger(processingTime='15 seconds')\
    .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Se abre en un ventana el directorio "source_scv_data" y se mueven los archivos que hay en su interior a otra ventana, la cual estara situada en "monitoring_data". Mientras que se recibe los archivos se ira mostrando en el terminal lo siguiente:
# MAGIC 
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 0
# MAGIC -------------------------------------------
# MAGIC +------------------------------------------+------+-----+
# MAGIC |window                                    |userA |count|
# MAGIC +------------------------------------------+------+-----+
# MAGIC |[2012-07-05 17:06:00, 2012-07-05 17:07:00]|223125|1    |
# MAGIC |[2012-07-04 02:55:30, 2012-07-04 02:56:30]|4543  |1    |
# MAGIC |[2012-07-05 15:54:20, 2012-07-05 15:55:20]|305032|1    |
# MAGIC |[2012-07-05 17:06:10, 2012-07-05 17:07:10]|223125|1    |
# MAGIC |[2012-07-02 16:11:00, 2012-07-02 16:12:00]|69958 |1    |
# MAGIC |[2012-07-04 01:36:10, 2012-07-04 01:37:10]|78359 |1    |
# MAGIC |[2012-07-06 11:28:20, 2012-07-06 11:29:20]|42318 |1    |
# MAGIC |[2012-07-05 17:05:40, 2012-07-05 17:06:40]|223125|1    |
# MAGIC |[2012-07-05 17:05:30, 2012-07-05 17:06:30]|223125|1    |
# MAGIC |[2012-07-04 03:22:40, 2012-07-04 03:23:40]|38011 |1    |
# MAGIC |[2012-07-06 11:28:50, 2012-07-06 11:29:50]|42318 |1    |
# MAGIC |[2012-07-04 03:22:30, 2012-07-04 03:23:30]|38011 |1    |
# MAGIC |[2012-07-04 03:22:00, 2012-07-04 03:23:00]|38011 |1    |
# MAGIC |[2012-07-05 17:06:20, 2012-07-05 17:07:20]|223125|1    |
# MAGIC |[2012-07-04 03:22:10, 2012-07-04 03:23:10]|38011 |1    |
# MAGIC |[2012-07-04 02:55:00, 2012-07-04 02:56:00]|4543  |1    |
# MAGIC |[2012-07-05 15:54:30, 2012-07-05 15:55:30]|305032|1    |
# MAGIC |[2012-07-04 10:47:50, 2012-07-04 10:48:50]|330734|1    |
# MAGIC |[2012-07-05 15:54:10, 2012-07-05 15:55:10]|305032|1    |
# MAGIC |[2012-07-06 11:29:00, 2012-07-06 11:30:00]|42318 |1    |
# MAGIC |[2012-07-04 10:47:30, 2012-07-04 10:48:30]|330734|1    |
# MAGIC |[2012-07-02 16:10:50, 2012-07-02 16:11:50]|69958 |1    |
# MAGIC |[2012-07-04 02:55:50, 2012-07-04 02:56:50]|4543  |1    |
# MAGIC |[2012-07-06 11:28:40, 2012-07-06 11:29:40]|42318 |1    |
# MAGIC |[2012-07-04 03:22:50, 2012-07-04 03:23:50]|38011 |1    |
# MAGIC |[2012-07-04 01:36:40, 2012-07-04 01:37:40]|78359 |1    |
# MAGIC |[2012-07-06 11:28:30, 2012-07-06 11:29:30]|42318 |1    |
# MAGIC |[2012-07-05 15:54:40, 2012-07-05 15:55:40]|305032|1    |
# MAGIC |[2012-07-02 16:10:30, 2012-07-02 16:11:30]|69958 |1    |
# MAGIC |[2012-07-04 10:48:10, 2012-07-04 10:49:10]|330734|1    |
# MAGIC |[2012-07-02 16:10:40, 2012-07-02 16:11:40]|69958 |1    |
# MAGIC |[2012-07-04 02:55:20, 2012-07-04 02:56:20]|4543  |1    |
# MAGIC |[2012-07-05 17:05:50, 2012-07-05 17:06:50]|223125|1    |
# MAGIC |[2012-07-04 03:22:20, 2012-07-04 03:23:20]|38011 |1    |
# MAGIC |[2012-07-04 10:48:00, 2012-07-04 10:49:00]|330734|1    |
# MAGIC |[2012-07-05 15:54:50, 2012-07-05 15:55:50]|305032|1    |
# MAGIC |[2012-07-04 10:47:40, 2012-07-04 10:48:40]|330734|1    |
# MAGIC |[2012-07-04 01:36:30, 2012-07-04 01:37:30]|78359 |1    |
# MAGIC |[2012-07-04 02:55:10, 2012-07-04 02:56:10]|4543  |1    |
# MAGIC |[2012-07-05 15:54:00, 2012-07-05 15:55:00]|305032|1    |
# MAGIC |[2012-07-04 01:36:00, 2012-07-04 01:37:00]|78359 |1    |
# MAGIC |[2012-07-06 11:29:10, 2012-07-06 11:30:10]|42318 |1    |
# MAGIC |[2012-07-04 10:47:20, 2012-07-04 10:48:20]|330734|1    |
# MAGIC |[2012-07-02 16:11:10, 2012-07-02 16:12:10]|69958 |1    |
# MAGIC |[2012-07-02 16:11:20, 2012-07-02 16:12:20]|69958 |1    |
# MAGIC |[2012-07-04 02:55:40, 2012-07-04 02:56:40]|4543  |1    |
# MAGIC |[2012-07-04 01:36:20, 2012-07-04 01:37:20]|78359 |1    |
# MAGIC |[2012-07-04 01:35:50, 2012-07-04 01:36:50]|78359 |1    |
# MAGIC +------------------------------------------+------+-----+
# MAGIC 
# MAGIC 2022-06-02 11:07:01 WARN  ProcessingTimeExecutor:66 - Current batch is falling behind. The trigger interval is 15000 milliseconds, but spent 16592 milliseconds
# MAGIC -------------------------------------------                                     
# MAGIC Batch: 1
# MAGIC -------------------------------------------
# MAGIC +------------------------------------------+------+-----+
# MAGIC |window                                    |userA |count|
# MAGIC +------------------------------------------+------+-----+
# MAGIC |[2012-07-05 17:06:00, 2012-07-05 17:07:00]|223125|1    |
# MAGIC |[2012-07-04 02:55:30, 2012-07-04 02:56:30]|4543  |1    |
# MAGIC |[2012-07-05 15:54:20, 2012-07-05 15:55:20]|305032|1    |
# MAGIC |[2012-07-05 17:06:10, 2012-07-05 17:07:10]|223125|1    |
# MAGIC |[2012-07-02 16:11:00, 2012-07-02 16:12:00]|69958 |1    |
# MAGIC |[2012-07-04 01:36:10, 2012-07-04 01:37:10]|78359 |1    |
# MAGIC |[2012-07-06 11:28:20, 2012-07-06 11:29:20]|42318 |1    |
# MAGIC |[2012-07-05 17:05:40, 2012-07-05 17:06:40]|223125|1    |
# MAGIC |[2012-07-05 17:05:30, 2012-07-05 17:06:30]|223125|1    |
# MAGIC |[2012-07-04 03:22:40, 2012-07-04 03:23:40]|38011 |1    |
# MAGIC |[2012-07-06 11:28:50, 2012-07-06 11:29:50]|42318 |1    |
# MAGIC |[2012-07-04 03:22:30, 2012-07-04 03:23:30]|38011 |1    |
# MAGIC |[2012-07-04 03:22:00, 2012-07-04 03:23:00]|38011 |1    |
# MAGIC |[2012-07-05 17:06:20, 2012-07-05 17:07:20]|223125|1    |
# MAGIC |[2012-07-04 03:22:10, 2012-07-04 03:23:10]|38011 |1    |
# MAGIC |[2012-07-04 02:55:00, 2012-07-04 02:56:00]|4543  |1    |
# MAGIC |[2012-07-05 15:54:30, 2012-07-05 15:55:30]|305032|1    |
# MAGIC |[2012-07-04 10:47:50, 2012-07-04 10:48:50]|330734|1    |
# MAGIC |[2012-07-05 15:54:10, 2012-07-05 15:55:10]|305032|1    |
# MAGIC |[2012-07-06 11:29:00, 2012-07-06 11:30:00]|42318 |1    |
# MAGIC |[2012-07-04 10:47:30, 2012-07-04 10:48:30]|330734|1    |
# MAGIC |[2012-07-02 16:10:50, 2012-07-02 16:11:50]|69958 |1    |
# MAGIC |[2012-07-04 02:55:50, 2012-07-04 02:56:50]|4543  |1    |
# MAGIC |[2012-07-06 11:28:40, 2012-07-06 11:29:40]|42318 |1    |
# MAGIC |[2012-07-04 01:36:40, 2012-07-04 01:37:40]|78359 |1    |
# MAGIC |[2012-07-04 03:22:50, 2012-07-04 03:23:50]|38011 |1    |
# MAGIC |[2012-07-06 11:28:30, 2012-07-06 11:29:30]|42318 |1    |
# MAGIC |[2012-07-05 15:54:40, 2012-07-05 15:55:40]|305032|1    |
# MAGIC |[2012-07-02 16:10:30, 2012-07-02 16:11:30]|69958 |1    |
# MAGIC |[2012-07-04 10:48:10, 2012-07-04 10:49:10]|330734|1    |
# MAGIC |[2012-07-02 16:10:40, 2012-07-02 16:11:40]|69958 |1    |
# MAGIC |[2012-07-04 02:55:20, 2012-07-04 02:56:20]|4543  |1    |
# MAGIC |[2012-07-05 17:05:50, 2012-07-05 17:06:50]|223125|1    |
# MAGIC |[2012-07-04 03:22:20, 2012-07-04 03:23:20]|38011 |1    |
# MAGIC |[2012-07-04 10:48:00, 2012-07-04 10:49:00]|330734|1    |
# MAGIC |[2012-07-05 15:54:50, 2012-07-05 15:55:50]|305032|1    |
# MAGIC |[2012-07-04 10:47:40, 2012-07-04 10:48:40]|330734|1    |
# MAGIC |[2012-07-04 01:36:30, 2012-07-04 01:37:30]|78359 |1    |
# MAGIC |[2012-07-04 02:55:10, 2012-07-04 02:56:10]|4543  |1    |
# MAGIC |[2012-07-05 15:54:00, 2012-07-05 15:55:00]|305032|1    |
# MAGIC |[2012-07-04 01:36:00, 2012-07-04 01:37:00]|78359 |1    |
# MAGIC |[2012-07-06 11:29:10, 2012-07-06 11:30:10]|42318 |1    |
# MAGIC |[2012-07-04 10:47:20, 2012-07-04 10:48:20]|330734|1    |
# MAGIC |[2012-07-02 16:11:10, 2012-07-02 16:12:10]|69958 |1    |
# MAGIC |[2012-07-02 16:11:20, 2012-07-02 16:12:20]|69958 |1    |
# MAGIC |[2012-07-04 02:55:40, 2012-07-04 02:56:40]|4543  |1    |
# MAGIC |[2012-07-04 01:36:20, 2012-07-04 01:37:20]|78359 |1    |
# MAGIC |[2012-07-04 01:35:50, 2012-07-04 01:36:50]|78359 |1    |
# MAGIC +------------------------------------------+------+-----+
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC # ejercicio 6 - modificado 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC mkdir csv_example6
# MAGIC 
# MAGIC mkdir example6_join
# MAGIC 
# MAGIC cd csv_example6
# MAGIC 
# MAGIC nano datos_ejemplo6.csv
# MAGIC 
# MAGIC <pre>
# MAGIC pepe,60,C/Toledo,45000
# MAGIC marta,65,C/Madrid,45000
# MAGIC juan,30,C/Limonero,45001
# MAGIC carmen,29,C/naranjo,45002
# MAGIC pepito,20,C/Madrid,45000
# MAGIC </pre>
# MAGIC 
# MAGIC cd ../
# MAGIC 
# MAGIC nano localidad.csv
# MAGIC 
# MAGIC <pre>
# MAGIC 45000,leganes
# MAGIC 45001,fuenlabrada
# MAGIC 45002,mostoles
# MAGIC </pre>

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.functions import window
from pyspark.sql.types import StructType

# COMMAND ----------

spark = SparkSession\
        .builder\
        .appName("StructuredCSVFile")\
        .getOrCreate()

# COMMAND ----------

windowSize = "60"
slideSize = "10"

windowDuration = '{} seconds'.format(windowSize)
slideDuration = '{} seconds'.format(slideSize)
monitoring_dir = 'example6_join'

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("InteractionCount")\
    .config("spark.eventLog.enabled","true")\
    .config("spark.eventLog.dir","applicationHistory")\
    .master("local[*]")\
    .getOrCreate()

# COMMAND ----------

userSchema = StructType().add("nombre","string")\
                            .add("edad","integer")\
                            .add("direccion","string")\
                            .add("cod_postal","string")

# COMMAND ----------

twitterIDSchema = StructType().add("cod_postal","string").add("nombre_loc","string")
twitterIDs = spark.read.schema(twitterIDSchema).csv('localidad.csv')
csvDF = spark\
    .readStream\
    .schema(userSchema)\
    .csv(monitoring_dir)

joinedDF = csvDF.join(twitterIDs,"cod_postal")

# COMMAND ----------

interactions = joinedDF.select(joinedDF['nombre'],joinedDF['cod_postal'],joinedDF['nombre_loc'])

# COMMAND ----------

windowedCounts = interactions\
                .groupBy(interactions.cod_postal,interactions.nombre_loc)\
                .count()

# COMMAND ----------

query = windowedCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .option('truncate','false')\
    .option('numRows','10000')\
    .trigger(processingTime='15 seconds')\
    .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Se abre en un ventana el directorio "csv_example6" y se mueven los archivos que hay en su interior a otra ventana, la cual estara situada en "example6_join". Mientras que se recibe los archivos se ira mostrando en el terminal lo siguiente:
# MAGIC 
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 0
# MAGIC -------------------------------------------
# MAGIC +----------+-----------+-----+
# MAGIC |cod_postal|nombre_loc |count|
# MAGIC +----------+-----------+-----+
# MAGIC |45001     |fuenlabrada|1    |
# MAGIC |45002     |mostoles   |1    |
# MAGIC |45000     |leganes    |3    |
# MAGIC +----------+-----------+-----+
# MAGIC </pre>