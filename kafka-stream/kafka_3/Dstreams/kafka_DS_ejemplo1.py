# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 1
# MAGIC cd Spark-Streaming-Python-Examples/
# MAGIC 
# MAGIC jupyter notebook Ejemplo\ 1\ -\ Spark\ Streaming\ con\ DStreams.ipynb 

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
pairs = words.map(lambda word:(word, 1))
wordCounts = pairs.reduceByKey(lambda x,y: x + y)

# COMMAND ----------

# Print the first ten elements of each RDD generated in this DStream to the console
#In python the command for output is pprint
wordCounts.pprint()

ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre><font color="#8AE234"><b>spark@spark-virtualBox</b></font>:<font color="#729FCF"><b>~/Spark-Streaming-Python-Examples</b></font>$ nc -lk 9999
# MAGIC hola mundo
# MAGIC esto es una prueba
# MAGIC esto es otra prueba
# MAGIC </pre>
# MAGIC 
# MAGIC pyspark
# MAGIC 
# MAGIC <pre>
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:20
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:25
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:30
# MAGIC -------------------------------------------
# MAGIC ('hola', 1)
# MAGIC ('mundo', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:35
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:40
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('es', 1)
# MAGIC ('otra', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:45
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 12:55:50
# MAGIC -------------------------------------------
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC # ejemplo1 - modificado

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

# Split each line into words
words =lines.flatMap(lambda line:line.split(" "))

# COMMAND ----------

# Filter words less than 3 characters
filteredWords = words.filter(lambda word : len(word) > 2)

# COMMAND ----------

# Count each word in each batch
pairs = filteredWords.map(lambda word:(word, 1))
wordCounts = pairs.reduceByKey(lambda x,y: x + y)

# COMMAND ----------

# Print the first ten elements of each RDD generated in this DStream to the console
#In python the command for output is pprint
wordCounts.pprint()

ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre><font color="#8AE234"><b>spark@spark-virtualBox</b></font>:<font color="#729FCF"><b>~/Spark-Streaming-Python-Examples</b></font>$ nc -lk 9999
# MAGIC hola mundo
# MAGIC esto es una prueba
# MAGIC esto es otra prueba
# MAGIC hi
# MAGIC this
# MAGIC is
# MAGIC a 
# MAGIC new
# MAGIC message
# MAGIC </pre>
# MAGIC 
# MAGIC pyspark
# MAGIC 
# MAGIC <pre>
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:06:20
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:06:25
# MAGIC -------------------------------------------
# MAGIC ('this', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:06:30
# MAGIC -------------------------------------------
# MAGIC ('new', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:06:35
# MAGIC -------------------------------------------
# MAGIC ('message', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:06:40
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC </pre>