# Databricks notebook source
# MAGIC %md
# MAGIC # ejemplo 3
# MAGIC jupyter notebook Ejemplo\ 3\ -\ Spark\ Streaming\ con\ DStreams.ipynb 

# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

# Function to create and setup a new StreamingContext
def functionToCreateContext():
    # Create a local StreamingContext with two working thread and batch interval of 5 seconds
    sc = SparkContext("local[2]", "WindowedNetworkWordCount")
    ssc = StreamingContext(sc, 5)
    
    # Mandatory set a checkpoint dir
    # http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
    #Crear carpeta /checkpointDirectory dentro del directorio notebooks-spark o dentro del directorio Spark-Streaming-Python-Examples
    ssc.checkpoint("./checkpointDirectory")  # set checkpoint directory
    return ssc

# COMMAND ----------

# Get StreamingContext from checkpoint data or create a new one
ssc = StreamingContext.getOrCreate(checkpointPath = "./checkpointDirectory", setupFunc = functionToCreateContext)

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

# COMMAND ----------

# Reduce last 30 seconds of data, every 10 seconds
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y:x-y, 30, 10)

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
# MAGIC <pre>hola mundo
# MAGIC esto es una prueba
# MAGIC para checkpoint
# MAGIC otro mensaje
# MAGIC </pre>
# MAGIC 
# MAGIC 
# MAGIC pyspark 
# MAGIC 
# MAGIC <pre>
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:45:15
# MAGIC -------------------------------------------
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:45:25
# MAGIC -------------------------------------------
# MAGIC ('hola', 1)
# MAGIC ('mundo', 1)
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('para', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC ('checkpoint', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:45:35
# MAGIC -------------------------------------------
# MAGIC ('hola', 1)
# MAGIC ('mundo', 1)
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('para', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC ('checkpoint', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:45:45
# MAGIC -------------------------------------------
# MAGIC ('hola', 1)
# MAGIC ('mundo', 1)
# MAGIC ('esto', 1)
# MAGIC ('prueba', 1)
# MAGIC ('para', 1)
# MAGIC ('es', 1)
# MAGIC ('una', 1)
# MAGIC ('checkpoint', 1)
# MAGIC 
# MAGIC -------------------------------------------
# MAGIC Time: 2022-06-01 13:45:55
# MAGIC -------------------------------------------
# MAGIC ('otro', 1)
# MAGIC ('mensaje', 1)
# MAGIC 
# MAGIC 
# MAGIC </pre>
# MAGIC 
# MAGIC carpeta checkpoint
# MAGIC 
# MAGIC <pre><font color="#8AE234"><b>spark@spark-virtualBox</b></font>:<font color="#729FCF"><b>~/Spark-Streaming-Python-Examples</b></font>$ cd checkpointDirectory/
# MAGIC <font color="#8AE234"><b>spark@spark-virtualBox</b></font>:<font color="#729FCF"><b>~/Spark-Streaming-Python-Examples/checkpointDirectory</b></font>$ ls<font color="#729FCF"><b>1643147d-e4d2-497b-808d-8647a9d9b1d9</b></font>  checkpoint-1654084090000
# MAGIC checkpoint-1654084070000              checkpoint-1654084095000
# MAGIC checkpoint-1654084075000              checkpoint-1654084095000.bk
# MAGIC checkpoint-1654084075000.bk           checkpoint-1654084100000
# MAGIC checkpoint-1654084080000              <font color="#729FCF"><b>e41df5b1-e089-44c5-b639-035a4d9b433d</b></font>
# MAGIC checkpoint-1654084085000              <font color="#729FCF"><b>receivedBlockMetadata</b></font>
# MAGIC checkpoint-1654084085000.bk
# MAGIC </pre>