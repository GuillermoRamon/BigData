# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 4
# MAGIC 
# MAGIC cd kafka_2.11-2.1.0/
# MAGIC 
# MAGIC zookeeper-server-start.sh config/zookeeper.properties
# MAGIC 
# MAGIC kafka-server-start.sh config/server.properties
# MAGIC 
# MAGIC jupyter notebook Ejemplo\ 4\ -\ Spark\ Structured\ Streaming.ipynb

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.functions import explode 
from pyspark.sql.functions import split

# COMMAND ----------

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic wordcount_topic --create --partitions 3 --replication-factor 1
# MAGIC 
# MAGIC <pre>Created topic &quot;wordcount_topic&quot;.</pre>

# COMMAND ----------

# Create DataSet representing the stream of input lines from kafka
# Es necesario de antemano haber creado el topic llamado wordcount_topic
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "wordcount_topic")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

# COMMAND ----------

# Split the lines into words 
words = lines.select( 
    # explode turns each item in an array into a separate row 
    explode( 
        split(lines.value, ' ') 
        ).alias('word') 
    )

# COMMAND ----------

# Generate running word count 
wordCounts = words.groupBy('word').count() 

# COMMAND ----------

# Start running the query that prints the running counts to the console
# Una vez iniciado el procesamiento del flujo empezar a insertar elementos al topic de kafka
# Ver resultados en la consola donde se ejecuto el jupyter notebook
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start() 

query.awaitTermination() 

# COMMAND ----------

# MAGIC %md
# MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic wordcount_topic
# MAGIC 
# MAGIC <pre>&gt;hola hola
# MAGIC &gt;esto es un mensaje para un topic
# MAGIC &gt;y esto es otro mensaje
# MAGIC &gt;
# MAGIC </pre>
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 1
# MAGIC -------------------------------------------
# MAGIC +----+-----+
# MAGIC |word|count|
# MAGIC +----+-----+
# MAGIC |hola|    2|
# MAGIC +----+-----+
# MAGIC 
# MAGIC [Stage 5:======================&gt;                                 (81 + 1) / 200][I 09:38:25.500 NotebookApp] Saving file at /Ejemplo 4 - Spark Structured Streaming.ipynb
# MAGIC -------------------------------------------                                     
# MAGIC Batch: 2
# MAGIC -------------------------------------------
# MAGIC +-------+-----+
# MAGIC |   word|count|
# MAGIC +-------+-----+
# MAGIC |   para|    1|
# MAGIC |mensaje|    1|
# MAGIC |     es|    1|
# MAGIC |   esto|    1|
# MAGIC |   hola|    2|
# MAGIC |     un|    2|
# MAGIC |  topic|    1|
# MAGIC +-------+-----+
# MAGIC 
# MAGIC -------------------------------------------                                     
# MAGIC Batch: 3
# MAGIC -------------------------------------------
# MAGIC +-------+-----+
# MAGIC |   word|count|
# MAGIC +-------+-----+
# MAGIC |   para|    1|
# MAGIC |mensaje|    2|
# MAGIC |   otro|    1|
# MAGIC |     es|    2|
# MAGIC |   esto|    2|
# MAGIC |      y|    1|
# MAGIC |   hola|    2|
# MAGIC |     un|    2|
# MAGIC |  topic|    1|
# MAGIC +-------+-----+
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC # ejemplo 4 - modificado

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.functions import explode 
from pyspark.sql.functions import split
from pyspark.sql.functions import col, size
from pyspark.sql.functions import lower

# COMMAND ----------

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

# COMMAND ----------

# Create DataSet representing the stream of input lines from kafka
# Es necesario de antemano haber creado el topic llamado wordcount_topic
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "wordcount_topic")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

# COMMAND ----------

# Split the lines into words 
words = lines.select( 
    # explode turns each item in an array into a separate row 
    explode( 
        split(lines.value, ' ') 
        ).alias('word') 
    )

# COMMAND ----------

# Generate running word count
lowerWords = words.select(lower(col('word')).alias('palabra'))
filteredWords = lowerWords.filter(~col("palabra").isin(['es', 'un']))
wordCounts = filteredWords.groupBy('palabra').count() 

# COMMAND ----------

# Start running the query that prints the running counts to the console
# Una vez iniciado el procesamiento del flujo empezar a insertar elementos al topic de kafka
# Ver resultados en la consola donde se ejecuto el jupyter notebook
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start() 

query.awaitTermination() 

# COMMAND ----------

# MAGIC %md
# MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic wordcount_topic
# MAGIC 
# MAGIC <pre>&gt;ESTO ES UN mensaje De pruebA
# MAGIC &gt;
# MAGIC </pre>
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>-------------------------------------------                                     
# MAGIC Batch: 1
# MAGIC -------------------------------------------
# MAGIC +-------+-----+
# MAGIC |palabra|count|
# MAGIC +-------+-----+
# MAGIC | prueba|    1|
# MAGIC |mensaje|    1|
# MAGIC |     de|    1|
# MAGIC |   esto|    1|
# MAGIC +-------+-----+
# MAGIC </pre>