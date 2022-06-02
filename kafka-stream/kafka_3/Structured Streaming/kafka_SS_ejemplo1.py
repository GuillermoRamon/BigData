# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 1
# MAGIC jupyter notebook Ejemplo\ 1\ -\ Spark\ Structured\ Streaming.ipynb 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC In command line -> nc -lk 9999

# COMMAND ----------

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines =spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# COMMAND ----------

# Split the lines into words
words =lines.select(explode(split(lines.value, " ")).alias("word"))

# COMMAND ----------

# filtrando los artículos (el, la, los, las, un, una, unos, unas)
filteredWords = words.filter(~col("word").isin(['el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas']))

# COMMAND ----------

# Generate running word count
wordCounts =filteredWords.groupBy("word").count()

# COMMAND ----------

# Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>el mio       
# MAGIC la los 
# MAGIC tuyo suyo
# MAGIC </pre>
# MAGIC 
# MAGIC salida
# MAGIC 
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 0
# MAGIC -------------------------------------------
# MAGIC +----+-----+
# MAGIC |word|count|
# MAGIC +----+-----+
# MAGIC | mio|    1|
# MAGIC +----+-----+
# MAGIC 
# MAGIC -------------------------------------------                                     
# MAGIC Batch: 1
# MAGIC -------------------------------------------
# MAGIC +----+-----+
# MAGIC |word|count|
# MAGIC +----+-----+
# MAGIC |    |    1|
# MAGIC | mio|    1|
# MAGIC +----+-----+
# MAGIC 
# MAGIC [Stage 5:==========&gt;                                             (39 + 1) / 200][I 16:05:48.247 NotebookApp] Saving file at /Ejemplo 1 - Spark Structured Streaming.ipynb
# MAGIC -------------------------------------------                                     
# MAGIC Batch: 2
# MAGIC -------------------------------------------
# MAGIC +----+-----+
# MAGIC |word|count|
# MAGIC +----+-----+
# MAGIC |suyo|    1|
# MAGIC |tuyo|    1|
# MAGIC |    |    1|
# MAGIC | mio|    1|
# MAGIC +----+-----+
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC # ejemplo 1 - modificado

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import upper

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC In command line -> nc -lk 9999

# COMMAND ----------

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines =spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# COMMAND ----------

# Split the lines into words
words =lines.select(explode(split(lines.value, " ")).alias("word"))
# conver to capital letter
upperWords = words.select(upper(col('word')).alias('palabra'))

# COMMAND ----------

# filtrando los artículos (el, la, los, las, un, una, unos, unas)
filteredWords = upperWords.filter(~col("palabra").isin(['el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas']))

# COMMAND ----------

# Generate running word count
wordCounts =filteredWords.groupBy("palabra").count()

# COMMAND ----------

# Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>hola Hola HOLA holA
# MAGIC </pre>
# MAGIC 
# MAGIC salida
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 0
# MAGIC -------------------------------------------
# MAGIC +-------+-----+
# MAGIC |palabra|count|
# MAGIC +-------+-----+
# MAGIC |   HOLA|    4|
# MAGIC +-------+-----+
# MAGIC </pre>