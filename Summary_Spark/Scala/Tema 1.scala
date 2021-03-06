// Databricks notebook source
// MAGIC %md 
// MAGIC RDD
// MAGIC 
// MAGIC son grupos de datos de solo lectura generados tras realizar acciones en los datos originales. Permiten cargar gran cantidad de datos en memoria y dividirse para ser tratados de forma paralela, con esto se consigue realizar operaciones en grandes cantidades de datos de forma rápida y tolerante a fallos.
// MAGIC 
// MAGIC Context
// MAGIC 
// MAGIC es el principal punto de entrada de Spark , desde donde se crean el resto de variables.

// COMMAND ----------

// MAGIC %md
// MAGIC #Tema 1 del libro - A Unified Analytics Engine

// COMMAND ----------

// MAGIC %md
// MAGIC ##Spark Components
// MAGIC Spark offers four distinct components as libraries for diverse
// MAGIC workloads: Spark SQL, Spark MLlib, Spark Structured Streaming, and GraphX.
// MAGIC 
// MAGIC Each of these components is separate from Spark’s core fault-tolerant engine, in that you
// MAGIC use APIs to write your Spark application and Spark converts this into a DAG that is
// MAGIC executed by the core engine. 
// MAGIC 
// MAGIC you write your Spark code using the provided Structured APIs( in Java, R, Scala, SQL, or Python)
// MAGIC the underlying code is decomposed into highly compact bytecode that is executed in the workers’ JVMs across the cluster

// COMMAND ----------

// MAGIC %md
// MAGIC ###SparkSQL
// MAGIC This module works well with structured data. You can read data stored in an RDBMS
// MAGIC table or from file formats with structured data (CSV, text, JSON, Avro, ORC, Parquet,
// MAGIC etc.) and then construct permanent or temporary tables in Spark
// MAGIC 
// MAGIC  For example, read from a JSON file stored on
// MAGIC Amazon S3, create a temporary table, and issue a SQL-like query

// COMMAND ----------

// In Scala
// Read data off Amazon S3 bucket into a Spark DataFrame
spark.read.json("s3://apache_spark/data/committers.json")
 .createOrReplaceTempView("committers")
// Issue a SQL query and return the result as a Spark DataFrame
val results = spark.sql("""SELECT name, org, module, release, num_commits
 FROM committers WHERE module = 'mllib' AND num_commits > 10
 ORDER BY num_commits DESC""")

// COMMAND ----------

// MAGIC %md
// MAGIC You can write similar code snippets in Python, R, or Java, and the generated bytecode
// MAGIC will be identical, resulting in the same performance

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Spark MLlib
// MAGIC library containing common machine learning (ML) algorithms called MLlib. These APIs 
// MAGIC allow you to extract or transform features, build pipelines (for training
// MAGIC and evaluating), and persist models (for saving and reloading them) during deployment. 
// MAGIC 
// MAGIC Also include the use of common linear algebra operations and
// MAGIC statistics. MLlib includes other low-level ML primitives, including a generic gradient
// MAGIC descent optimization

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark Structured Streaming
// MAGIC Necessary for big data developers to combine and react in real time to both static data
// MAGIC and streaming data. the new model views a stream as a continually growing table, with new rows of data
// MAGIC appended at the end. Developers can merely treat this as a structured table and issue
// MAGIC queries against it as they would a static table.

// COMMAND ----------

// MAGIC %md
// MAGIC ###GraphX
// MAGIC GraphX is a library for manipulating graph. and performing graph-parallel computations.
// MAGIC It offers the standard graph algorithms for analysis, connections, and traversals 

// COMMAND ----------

// MAGIC %md
// MAGIC This code snippet shows a simple example of how to join two graphs using the
// MAGIC GraphX APIs:

// COMMAND ----------

// In Scala
val graph = Graph(vertices, edges)
messages = spark.textFile("hdfs://...")
val graph2 = graph.joinVertices(messages) {
 (id, vertex, msg) => ...
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Apache Spark’s Distributed Execution
// MAGIC  At a high level in the Spark architecture, a Spark
// MAGIC application consists of a driver program that is responsible for orchestrating parallel
// MAGIC operations on the Spark cluster. The driver accesses the distributed components in
// MAGIC the cluster—the Spark executors and cluster manager—through a SparkSession.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark driver
// MAGIC Spark driver has multiple roles: it communicates with the cluster manager; it requests
// MAGIC resources (CPU, memory, etc.) from the cluster manager for Spark’s executors
// MAGIC (JVMs); and it transforms all the Spark operations into DAG computations, schedules them, and distributes their execution as tasks across the Spark executors. 

// COMMAND ----------

// MAGIC %md
// MAGIC ###SparkSession
// MAGIC An object that provides a point of entry to interact with underlying Spark func‐
// MAGIC tionality and allows programming Spark with its APIs. In an interactive Spark
// MAGIC shell, the Spark driver instantiates a SparkSession for you, while in a Spark
// MAGIC application, you create a SparkSession object yourself.
// MAGIC  
// MAGIC SparkSession provides a single unified entry point to all of
// MAGIC Spark’s functionality ( SparkContext,
// MAGIC SQLContext, HiveContext, SparkConf, and StreamingContext)
// MAGIC 
// MAGIC Example of sparksession:

// COMMAND ----------

// In Scala
import org.apache.spark.sql.SparkSession
// Build SparkSession
val spark = SparkSession
 .builder
 .appName("LearnSpark")
 .config("spark.sql.shuffle.partitions", 6)
 .getOrCreate()
...
// Use the session to read JSON 
val people = spark.read.json("...")
...
// Use the session to issue a SQL query
val resultsDF = spark.sql("SELECT city, pop, state, zip FROM table_name")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Cluster manager
// MAGIC The cluster manager is responsible for managing and allocating resources for the
// MAGIC cluster of nodes on which your Spark application runs. 
// MAGIC 
// MAGIC Spark supports
// MAGIC four cluster managers: the built-in standalone cluster manager, Apache Hadoop
// MAGIC YARN, Apache Mesos, and Kubernetes.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark executor
// MAGIC A Spark executor runs on each worker node in the cluster.
// MAGIC 
// MAGIC The executors communi‐
// MAGIC cate with the driver program and are responsible for executing tasks on the workers.
// MAGIC In most deployments modes, only a single executor runs per node.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Distributed data and partitions
// MAGIC Actual physical data is distributed across storage as partitions residing in either HDFS or cloud storage (see Figure 1-5). While the data is distributed as partitions across the physical cluster, Spark treats each partition as a high-level logical data abstraction—as
// MAGIC a DataFrame in memory.
// MAGIC 
// MAGIC each Spark executor is preferably allocated a task that requires it to read the partition closest to it in the network. Partitioning allows for efficient parallelism.
// MAGIC Them allows Spark executors to process only data that is close to
// MAGIC them, minimizing network bandwidth.