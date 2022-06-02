# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 2
# MAGIC jupyter notebook Ejemplo\ 2\ -\ Spark\ Streaming\ con\ DStreams.ipynb 

# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

# Create a local StreamingContext with two working thread and batch interval of 5 seconds
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)

# COMMAND ----------

# MAGIC %md
# MAGIC In command line -> nc -lk 9999

# COMMAND ----------

# Create a DStream that will connect to hostname:port, like localhost:9999
lines =ssc.socketTextStream("localhost", 9999)

# COMMAND ----------

# Split each line into words
words =lines.flatMap(lambda line:line.split(" "))

# COMMAND ----------

# Count each word in each batch
wordCounts = words.countByValue()

# COMMAND ----------

# Print the first ten elements of each RDD generated in this DStream to the console
#In python the command for output is pprint
wordCounts.pprint()

ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate

# COMMAND ----------

# MAGIC %md
# MAGIC terminal:
# MAGIC 
# MAGIC <pre><font color="#8AE234"><b>spark@spark-virtualBox</b></font>:<font color="#729FCF"><b>~/Spark-Streaming-Python-Examples</b></font>$ nc -lk 9999
# MAGIC hola hola
# MAGIC esto es otra prueba
# MAGIC 
# MAGIC </pre>
# MAGIC 
# MAGIC pyspark:
# MAGIC <pre>
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:25:25
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:25:30
# MAGIC -------------------------------------------
# MAGIC ('hola', 2)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:25:35
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('es', 1)
# MAGIC ('otra', 1)
# MAGIC </pre>