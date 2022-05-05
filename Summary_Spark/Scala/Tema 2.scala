// Databricks notebook source
// MAGIC %md
// MAGIC #Tema 2 del libro - Getting Started

// COMMAND ----------

spark.version

// COMMAND ----------

Let’s look at a short example where we read in a text file as a DataFrame, show a sample of the strings read, and count the total number of lines in the file.

// COMMAND ----------

val strings = spark.read.text("/FileStore/tables/README.md")


// COMMAND ----------

// MAGIC %md
// MAGIC The show(10, false) operation on the DataFrame only displays the
// MAGIC first 10 lines without truncating; by default the truncate Boolean flag is true

// COMMAND ----------

val strings = spark.read.text("/FileStore/tables/README.md")
strings.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Understanding Spark Application Concepts

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark Application and SparkSession
// MAGIC At the core of every Spark application is the Spark driver program, which creates a
// MAGIC SparkSession object. When you’re working with a Spark shell, the driver is part of
// MAGIC the shell and the SparkSession object is created for
// MAGIC you.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark Jobs
// MAGIC the driver converts your Spark applica‐
// MAGIC tion into one or more Spark jobs.  It then transforms each job into a
// MAGIC DAG.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark Stages
// MAGIC stages are created based on what operations can be performed serially or in parallel.  they may be divided into multiple stages.
// MAGIC 
// MAGIC En comparación con MapReduce, el cual crea un DAG con dos estados predefinidos (Map y Reduce), los grafos DAG creados por Spark pueden tener cualquier número de etapas. Spark con DAG es más rápido que MapReduce por el hecho de que no tiene que escribir en disco los resultados obtenidos en las etapas intermedias del grafo. MapReduce, sin embargo, debe escribir en disco los resultados entre las etapas Map y Reduce. Gracias a esto es posible programar complejos hilos de ejecución paralelos en unas pocas líneas de código

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark Tasks
// MAGIC  each task maps to a single core and works on a single partition of data. an executor with 16 cores can have 16 or more
// MAGIC tasks working on 16 or more partitions in parallel

// COMMAND ----------

// MAGIC %md
// MAGIC ##Transformations, Actions, and Lazy Evaluation
// MAGIC Spark operations on distributed data can be classified into two types: transformations
// MAGIC and actions.
// MAGIC 
// MAGIC Transformations, as the name suggests, transform a Spark DataFrame
// MAGIC into a new DataFrame without altering the original data, giving it the property of
// MAGIC immutability. All transformations are evaluated lazily. That is, their results are not computed immediately, but they are recorded or remembered as a lineage. all transformations T are recorded until the action A is invoked. (Inmutable: no se puede modificar una vez creado)
// MAGIC 
// MAGIC The action is what triggers the execution of all transformations recorded as part of the query execution plan. 
// MAGIC 
// MAGIC In this example, nothing happens until filtered.count() is executed in the
// MAGIC shell:

// COMMAND ----------

// In Scala
import org.apache.spark.sql.functions._
//scala> val strings = spark.read.text("C:/spark/spark-3.0.3-bin-hadoop2.7/README.md")
val strings = spark.read.text("/FileStore/tables/README.md")
val filtered = strings.filter(col("value").contains("Spark"))
filtered.count()
//res5: Long = 20

// COMMAND ----------

// MAGIC %md
// MAGIC ##The Spark UI
// MAGIC Spark includes a graphical user interface that you can use to inspect or monitor Spark applications in their various stages of decomposition—that is jobs, stages, and tasks. 
// MAGIC Depending on how Spark is deployed, the driver launches a web UI, running by
// MAGIC default on port 4040, where you can view metrics and details such as:
// MAGIC 
// MAGIC • A list of scheduler stages and tasks
// MAGIC 
// MAGIC • A summary of RDD sizes and memory usage
// MAGIC 
// MAGIC • Information about the environment
// MAGIC 
// MAGIC • Information about the running executors
// MAGIC 
// MAGIC • All the Spark SQL queries

// COMMAND ----------

// MAGIC %md
// MAGIC ##Counting M&Ms for the Cookie Monster
// MAGIC If the file were huge, it would be
// MAGIC distributed across a cluster partitioned into small chunks of data, and our Spark program would distribute the task of counting each word in each partition and return us
// MAGIC the final aggregated count.
// MAGIC 
// MAGIC Let’s write a Spark program that reads a file with over 100,000 entries (where each
// MAGIC row or line has a <state, mnm_color, count>) and computes and aggregates the
// MAGIC counts for each color and state.  These aggregated counts tell us the colors of M&Ms
// MAGIC favored by students in each state.

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("MnMCount").getOrCreate()

val mnmDF = spark.read
.option("header","true")
.option("inferSchema","true")
.csv("/FileStore/tables/mnm_dataset.csv")

val countMnMDF=mnmDF
.select("State","Color","Count")
.groupBy("State","Color")
.agg(count("Count").alias("Total"))
.orderBy(desc("Total"))

countMnMDF.show(60)
println(s"Total Rows = ${countMnMDF.count()}")
println()

val caCountMnMDF = mnmDF
 .select("State", "Color", "Count")
 .where(col("State") === "CA")
 .groupBy("State", "Color")
 .agg(count("Count").alias("Total"))
 .orderBy(desc("Total"))

 caCountMnMDF.show(10)