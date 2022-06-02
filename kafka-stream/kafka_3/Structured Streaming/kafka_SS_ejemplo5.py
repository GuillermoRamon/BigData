# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 5
# MAGIC jupyter notebook Ejemplo\ 5\ -\ Spark\ Structured\ Streaming.ipynb

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaJSON")\
        .getOrCreate()

# COMMAND ----------

# Define schema of json
schema = StructType() \
        .add("nombre", StringType()) \
        .add("edad", IntegerType()) \
        .add("peso", FloatType()) \
        .add("direccion", StringType())

# COMMAND ----------

# Create DataSet representing the stream of input lines from kafka
# Es necesario de antemano haber creado el topic llamado wordcount_topic
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "json_topic")\
        .load()\
        .selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), schema).alias("parsed_value"))\
        .select("parsed_value.*")

# COMMAND ----------

# Una vez iniciado el procesamiento del flujo empezar a insertar elementos al topic de kafka
# Ver resultados en la consola donde se ejecuto el jupyter notebook
query = lines.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic json_topic
# MAGIC 
# MAGIC <pre>&gt;{&quot;nombre&quot;:&quot;JOSE ANTONIO&quot;,&quot;edad&quot;:38,&quot;peso&quot;:75.5,&quot;direccion&quot;:&quot;C/ DEL LIMONERO 39, PISO 1-A&quot;}
# MAGIC &gt;
# MAGIC </pre>
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>-------------------------------------------
# MAGIC Batch: 1
# MAGIC -------------------------------------------
# MAGIC +------------+----+----+--------------------+
# MAGIC |      nombre|edad|peso|           direccion|
# MAGIC +------------+----+----+--------------------+
# MAGIC |JOSE ANTONIO|  38|75.5|C/ DEL LIMONERO 3...|
# MAGIC +------------+----+----+--------------------+
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC # ejemplo 5 - modificado

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaJSON")\
        .getOrCreate()

# COMMAND ----------

# Define schema of json
schema = StructType() \
        .add("nombre", StringType()) \
        .add("edad", IntegerType()) \
        .add("peso", FloatType()) \
        .add("direccion", StringType()) \
        .add("altura", FloatType())

# COMMAND ----------

# Create DataSet representing the stream of input lines from kafka
# Es necesario de antemano haber creado el topic llamado wordcount_topic
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "json_topic")\
        .load()\
        .selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), schema).alias("parsed_value"))\
        .select("parsed_value.*")
lines2 = lines.withColumn("IMC",col("peso")/(col("altura")*col("altura")))

# COMMAND ----------

# Una vez iniciado el procesamiento del flujo empezar a insertar elementos al topic de kafka
# Ver resultados en la consola donde se ejecuto el jupyter notebook
query = lines2.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic json_topic
# MAGIC 
# MAGIC <pre>&gt;{&quot;nombre&quot;:&quot;JOSE ANTONIO&quot;,&quot;edad&quot;:38,&quot;peso&quot;:75.5,&quot;direccion&quot;:&quot;C/ DEL LIMONERO 39, PISO 1-A&quot;,&quot;altura&quot;:1.75}
# MAGIC </pre>
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>-------------------------------------------
# MAGIC Batch: 3
# MAGIC -------------------------------------------
# MAGIC +------------+----+----+--------------------+------+------------------+
# MAGIC |      nombre|edad|peso|           direccion|altura|               IMC|
# MAGIC +------------+----+----+--------------------+------+------------------+
# MAGIC |JOSE ANTONIO|  38|75.5|C/ DEL LIMONERO 3...|  1.75|24.653061224489797|
# MAGIC +------------+----+----+--------------------+------+------------------+
# MAGIC </pre>