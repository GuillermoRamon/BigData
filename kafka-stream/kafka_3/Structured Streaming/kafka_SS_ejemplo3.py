# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 3
# MAGIC 
# MAGIC jupyter notebook Ejemplo\ 3\ -\ Spark\ Structured\ Streaming.ipynb

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

spark = SparkSession.builder.appName("StructuredNetworkWordCountWindowed").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC In command line -> nc -lk 9999

# COMMAND ----------

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines =spark.readStream.format("socket").option("host", "localhost").option("port", 9999).option('includeTimestamp', 'true').load()

# COMMAND ----------

# Split the lines into words
words =lines.select(explode(split(lines.value, " ")).alias("word"), lines.timestamp)

# COMMAND ----------

# Group the data by window and word and compute the count of each group
# Se utiliza la columna timestamp  añadida mediante la opción -> option('includeTimestamp', 'true')
# como marca de agua para a partir de esta campo hacer el filtrado 
# de todos aquellos registros que lleguen 10 minutos tarde con respecto a la ventana de tiempo
windowedCounts = words \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(words.timestamp, "10 minutes", "5 minutes"),
        words.word) \
    .count()

# COMMAND ----------

# Start running the query that prints the running counts to the console
# Ver resultados en la consola donde se ejecuto el jupyter notebook
query = windowedCounts.writeStream.outputMode("complete").format("console").option('truncate', 'false').start()
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 2
# MAGIC -------------------------------------------
# MAGIC +------------------------------------------+-------+-----+
# MAGIC |window                                    |word   |count|
# MAGIC +------------------------------------------+-------+-----+
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|prueba |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|de     |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|mensaje|3    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|para   |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|esto   |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|es     |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|hola   |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|un     |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|ejemplo|1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|hola   |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|para   |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|esto   |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|ejemplo|1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|otro   |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|un     |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|prueba |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|otro   |2    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|es     |2    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|de     |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|mensaje|3    |
# MAGIC +------------------------------------------+-------+-----+
# MAGIC </pre>