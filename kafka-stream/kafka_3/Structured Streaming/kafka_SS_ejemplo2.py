# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 2
# MAGIC jupyter notebook Ejemplo\ 2\ -\ Spark\ Structured\ Streaming.ipynb

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

spark = SparkSession.builder.appName("StructuredNetworkWordCountWindowed").getOrCreate()

# COMMAND ----------

# Create DataFrame representing the stream of input lines from connection to localhost:9999
# In command line -> nc -lk 9999
lines =spark.readStream.format("socket").option("host", "localhost").option("port", 9999).option('includeTimestamp', 'true').load()

# COMMAND ----------

# Split the lines into words
words =lines.select(explode(split(lines.value, " ")).alias("word"), lines.timestamp)

# COMMAND ----------

# Group the data by window and word and compute the count of each group 
windowedCounts = words.groupBy(window(words.timestamp, "10 minutes", "5 minutes"), words.word).count().orderBy('window')

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
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|otra   |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|mundo  |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|es     |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|pruebas|1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|hola   |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|esto   |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|prueba |2    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|una    |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|mas    |1    |
# MAGIC |[1970-01-20 04:20:00, 1970-01-20 04:30:00]|y      |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|prueba |2    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|otra   |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|una    |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|esto   |2    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|mundo  |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|hola   |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|pruebas|1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|mas    |1    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|es     |2    |
# MAGIC |[1970-01-20 04:25:00, 1970-01-20 04:35:00]|y      |1    |
# MAGIC +------------------------------------------+-------+-----+
# MAGIC </pre>