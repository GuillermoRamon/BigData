# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 4
# MAGIC jupyter notebook Ejemplo\ 4\ -\ Spark\ Streaming\ con\ DStreams.ipynb 

# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

# Function to create and setup a new StreamingContext
def functionToCreateContext():
    # Create a local StreamingContext with two working thread and batch interval of 5 seconds
    sc = SparkContext("local[2]", "WindowedNetworkWordCount2")
    ssc = StreamingContext(sc, 5)
    
    # Mandatory set a checkpoint dir
    # http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
    # Crear carpeta /checkpointDirectory2 dentro del directorio notebooks-spark o dentro del directorio Spark-Streaming-Python
    ssc.checkpoint("./checkpointDirectory2")  # set checkpoint directory
    return ssc

# COMMAND ----------

# Get StreamingContext from checkpoint data or create a new one
ssc = StreamingContext.getOrCreate(checkpointPath = "./checkpointDirectory2", setupFunc = functionToCreateContext)

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
windowedWordCounts = words.countByValueAndWindow(30, 10)

# COMMAND ----------

# Print the first ten elements of each RDD generated in this DStream to the console
windowedWordCounts.pprint()

ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC terminal
# MAGIC 
# MAGIC <pre>hola hola   
# MAGIC esto es una prueba
# MAGIC para el ejemplo 4
# MAGIC esto es otra prueba
# MAGIC sigo probando
# MAGIC </pre>
# MAGIC 
# MAGIC pyspark
# MAGIC 
# MAGIC <pre>
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 14:18:15
# MAGIC -------------------------------------------
# MAGIC ('hola', 2)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 14:18:25
# MAGIC -------------------------------------------
# MAGIC ('hola', 2)
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 14:18:35
# MAGIC -------------------------------------------
# MAGIC ('hola', 2)
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('para', 1)
# MAGIC ('el', 1)
# MAGIC ('ejemplo', 1)
# MAGIC ('4', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 14:18:45
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('para', 1)
# MAGIC ('el', 1)
# MAGIC ('ejemplo', 1)
# MAGIC ('4', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 14:18:55
# MAGIC -------------------------------------------
# MAGIC ('para', 1)
# MAGIC ('el', 1)
# MAGIC ('ejemplo', 1)
# MAGIC ('4', 1)
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('es', 1)
# MAGIC ('otra', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 14:19:05
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('sigo', 1)
# MAGIC ('probando', 1)
# MAGIC ('es', 1)
# MAGIC ('otra', 1)
# MAGIC </pre>
# MAGIC 
# MAGIC carpeta checkpoint
# MAGIC 
# MAGIC <pre><font color="#8AE234"><b>spark@spark-virtualBox</b></font>:<font color="#729FCF"><b>~/Spark-Streaming-Python-Examples/checkpointDirectory2</b></font>$ ls
# MAGIC <font color="#729FCF"><b>3c7bb4eb-8092-4686-8e9d-d28946689404</b></font>  checkpoint-1654086015000.bk
# MAGIC <font color="#729FCF"><b>8872916d-9a24-47da-89db-78f227379c22</b></font>  checkpoint-1654086020000
# MAGIC checkpoint-1654086000000              checkpoint-1654086025000
# MAGIC checkpoint-1654086005000              checkpoint-1654086025000.bk
# MAGIC checkpoint-1654086005000.bk           checkpoint-1654086030000
# MAGIC checkpoint-1654086010000              <font color="#729FCF"><b>receivedBlockMetadata</b></font>
# MAGIC checkpoint-1654086015000
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC # ejemplo 4 - modificado

# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

# Function to create and setup a new StreamingContext
def functionToCreateContext():
    # Create a local StreamingContext with two working thread and batch interval of 5 seconds
    sc = SparkContext("local[2]", "WindowedNetworkWordCount2")
    ssc = StreamingContext(sc, 5)
    
    # Mandatory set a checkpoint dir
    # http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
    # Crear carpeta /checkpointDirectory2 dentro del directorio notebooks-spark o dentro del directorio Spark-Streaming-Python
    ssc.checkpoint("./checkpointDirectory2_2")  # set checkpoint directory
    return ssc

# COMMAND ----------

# Get StreamingContext from checkpoint data or create a new one
ssc = StreamingContext.getOrCreate(checkpointPath = "./checkpointDirectory2_2", setupFunc = functionToCreateContext)

# COMMAND ----------

# MAGIC %md
# MAGIC In command line -> nc -lk 9999

# COMMAND ----------

# Create a DStream that will connect to hostname:port, like localhost:9999
lines =ssc.socketTextStream("localhost", 9999)

# COMMAND ----------

# Split each line into words
words =lines.flatMap(lambda line:line.split(" "))
filteredWords = words.filter(lambda word : len(word) > 2)

# COMMAND ----------

# Count each word in each batch
windowedWordCounts = filteredWords.countByValueAndWindow(20, 15)

# COMMAND ----------

# Print the first ten elements of each RDD generated in this DStream to the console
windowedWordCounts.pprint()

ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 15:36:00
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 15:36:15
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('una', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 15:36:30
# MAGIC -------------------------------------------
# MAGIC ('prueba', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 15:36:45
# MAGIC -------------------------------------------
# MAGIC ('esto', 1)
# MAGIC ('otr', 1)
# MAGIC ('mensaje', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 15:37:00
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 15:37:15
# MAGIC -------------------------------------------
# MAGIC ('hola', 1)
# MAGIC ('mundo', 1)
# MAGIC 
# MAGIC 
# MAGIC </pre>