// Databricks notebook source
// MAGIC %md
// MAGIC #Tema 3 del libro - Structured APIs

// COMMAND ----------

// MAGIC %md
// MAGIC ##Spark: What’s Underneath an RDD?
// MAGIC The RDD is the most basic abstraction in Spark. There are three vital characteristics
// MAGIC associated with an RDD:
// MAGIC 
// MAGIC • Dependencies
// MAGIC 
// MAGIC • Partitions (with some locality information)
// MAGIC 
// MAGIC • Compute function: Partition => Iterator[T]
// MAGIC 
// MAGIC First, a list of dependencies that instructs
// MAGIC Spark how an RDD is constructed with its inputs is required. When necessary to
// MAGIC reproduce results, Spark can recreate an RDD from these dependencies and replicate
// MAGIC operations on it. This characteristic gives RDDs resiliency.
// MAGIC 
// MAGIC Second, partitions provide Spark the ability to split the work to parallelize computation on partitions across executors. In some cases—for example, reading from HDFS—Spark will use locality information to send work to executors close to the data. That way less data is transmitted over the network.
// MAGIC 
// MAGIC And finally, an RDD has a compute function that produces an Iterator[T] for the
// MAGIC data that will be stored in the RDD

// COMMAND ----------

// MAGIC %md
// MAGIC ##Structuring Spark

// COMMAND ----------

// MAGIC %md
// MAGIC ###Key Merits and Benefits
// MAGIC Structure yields a number of benefits, including better performance and space effi‐
// MAGIC ciency across Spark components. Like DataFrame and dataSet, but there are other advantages: expressivity, simplicity, composability, and uniformity.

// COMMAND ----------

// MAGIC %md
// MAGIC we are using high-level DSL operators and APIs to tell Spark what to
// MAGIC do. Spark can inspect or parse this query and understand our intention, it can optimize
// MAGIC or arrange the operations for efficient execution.

// COMMAND ----------

// In Scala
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
// Create a DataFrame using SparkSession
val spark = SparkSession
 .builder
 .appName("AuthorsAges")
 .getOrCreate()
// Create a DataFrame of names and ages
val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
 ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
// Group the same names together, aggregate their ages, and compute an average
val avgDF = dataDF.groupBy("name").agg(avg("age"))
// Show the results of the final execution
avgDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##The DataFrame API
// MAGIC Inspired by pandas DataFrames in structure, format, and a few specific operations,
// MAGIC Spark DataFrames are like distributed in-memory tables with named columns and
// MAGIC schemas. When data is visualized as a structured table, it’s not only easy to digest but also easy
// MAGIC to work with when it comes to common operations you might want to execute on
// MAGIC rows and columns.
// MAGIC 
// MAGIC You can add or change
// MAGIC the names and data types of the columns, creating new DataFrames while the previous versions are preserved.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark’s Basic Data Types
// MAGIC Spark supports basic internal data
// MAGIC types. These data types can be declared in your Spark application or defined in your
// MAGIC schema. For example, in Scala, you can define or declare a particular column name to
// MAGIC be of type String, Byte, Long, or Map, etc.

// COMMAND ----------

// MAGIC %md
// MAGIC Spark supports similar basic Python data types

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark’s Structured and Complex Data Types
// MAGIC For complex data analytics, you won’t deal only with simple or basic data types. Your
// MAGIC data will be complex.They come in many forms: maps, arrays, structs, dates,
// MAGIC timestamps, fields, etc. 

// COMMAND ----------

// MAGIC %md
// MAGIC Here, we define variable names tied to a
// MAGIC Spark data type:

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
val nameTypes = StringType
//nameTypes: org.apache.spark.sql.types.StringType.type = StringType
val firstName = nameTypes
//firstName: org.apache.spark.sql.types.StringType.type = StringType
val lastName = nameTypes
//lastName: org.apache.spark.sql.types.StringType.type = StringType

// COMMAND ----------

// MAGIC %md
// MAGIC ###Schemas and Creating DataFrames
// MAGIC Defining a schema
// MAGIC up front as opposed to taking a schema-on-read approach offers three benefits:
// MAGIC 
// MAGIC • You relieve Spark from the onus of inferring data types.
// MAGIC 
// MAGIC • You prevent Spark from creating a separate job just to read a large portion of your file to ascertain the schema, which for a large data file can be expensive and time-consuming.
// MAGIC 
// MAGIC • You can detect errors early if data doesn’t match the schema.
// MAGIC 
// MAGIC you to always define your schema up front whenever you want to
// MAGIC read a large file from a data source.

// COMMAND ----------

// MAGIC %md
// MAGIC Spark allows you to define a schema in two ways. One is to define it programmatically, and the other is to employ a Data Definition Language (DDL) string, which is
// MAGIC much simpler and easier to read.

// COMMAND ----------

// MAGIC %md
// MAGIC To define a schema programmatically for a DataFrame with three named columns,
// MAGIC author, title, and pages, you can use the Spark DataFrame API. For example:

// COMMAND ----------

// In Scala
import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("author", StringType, false),
 StructField("title", StringType, false),
 StructField("pages", IntegerType, false)))

// COMMAND ----------

// MAGIC %md
// MAGIC Defining the same schema using DDL is much simpler:

// COMMAND ----------

// In Scala
val schema = "author STRING, title STRING, pages INT"

// COMMAND ----------

// MAGIC %md
// MAGIC the same code with a Scala example, this time reading from a JSON file:

// COMMAND ----------

// In Scala
package main.scala.chapter3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object Example3_7 {
def main(args: Array[String]) {
 val spark = SparkSession
 .builder
 .appName("Example-3_7")
 .getOrCreate()

 // Get the path to the JSON file
 val jsonFile = "/FileStore/tables/blogs.json"
 // Define our schema programmatically
 val schema = StructType(Array(StructField("Id", IntegerType, false),
 StructField("First", StringType, false),
 StructField("Last", StringType, false),
 StructField("Url", StringType, false),
 StructField("Published", StringType, false),
 StructField("Hits", IntegerType, false),
 StructField("Campaigns", ArrayType(StringType), false)))
 // Create a DataFrame by reading from the JSON file 
 // with a predefined schema
 val blogsDF = spark.read.schema(schema).json(jsonFile)
 // Show the DataFrame schema as output
 blogsDF.show(false)
 // Print the schema
 println(blogsDF.printSchema)
 println(blogsDF.schema)
}
}

// COMMAND ----------

// MAGIC %md
// MAGIC ###Columns and Expressions
// MAGIC Let’s take a look at some examples of what we can do with columns in Spark. Each
// MAGIC example is followed by its output:

// COMMAND ----------

// In Scala 
scala> import org.apache.spark.sql.functions._
scala> blogsDF.columns
res2: Array[String] = Array(Campaigns, First, Hits, Id, Last, Published, Url)
// Access a particular column with col and it returns a Column type
scala> blogsDF.col("Id")
res3: org.apache.spark.sql.Column = id
// Use an expression to compute a value
scala> blogsDF.select(expr("Hits * 2")).show(2)
// or use col to compute value
scala> blogsDF.select(col("Hits") * 2).show(2)

// COMMAND ----------

+----------+
|(Hits * 2)|
+----------+
|      9070|
|     17816|
+----------+

// COMMAND ----------

// Use an expression to compute big hitters for blogs
// This adds a new column, Big Hitters, based on the conditional expression
blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()


// COMMAND ----------

+---+---------+-------+---+---------+-----+--------------------+-----------+
| Id| First| Last|Url|Published| Hits| Campaigns|Big Hitters|
+---+---------+-------+---+---------+-----+--------------------+-----------+
| 1| Jules| Damji|...| 1/4/2016| 4535| [twitter, LinkedIn]| false|
| 2| Brooke| Wenig|...| 5/5/2018| 8908| [twitter, LinkedIn]| false|
| 3| Denny| Lee|...| 6/7/2019| 7659|[web, twitter, FB...| false|
| 4|Tathagata| Das|...|5/12/2018|10568| [twitter, FB]| true|
| 5| Matei|Zaharia|...|5/14/2014|40578|[web, twitter, FB...| true|
| 6| Reynold| Xin|...| 3/2/2015|25568| [twitter, LinkedIn]| true|
+---+---------+-------+---+---------+-----+--------------------+-----------+

// COMMAND ----------

// Concatenate three columns, create a new column, and show the
// newly created concatenated column
blogsDF
 .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
 .select(col("AuthorsId"))
 .show(4)

// COMMAND ----------

+-------------+
| AuthorsId|
+-------------+
| JulesDamji1|
| BrookeWenig2|
| DennyLee3|
|TathagataDas4|
+-------------+

// COMMAND ----------

// These statements return the same value, showing that
// expr is the same as a col method call
blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)

// COMMAND ----------

+-----+
| Hits|
+-----+
| 4535|
| 8908|
+-----+

// COMMAND ----------

+--------------------+---------+-----+---+-------+---------+-----------------+
| Campaigns| First| Hits| Id| Last|Published| Url|
+--------------------+---------+-----+---+-------+---------+-----------------+
| [twitter, LinkedIn]| Reynold|25568| 6| Xin| 3/2/2015|https://tinyurl.6|
|[web, twitter, FB...| Matei|40578| 5|Zaharia|5/14/2014|https://tinyurl.5|
| [twitter, FB]|Tathagata|10568| 4| Das|5/12/2018|https://tinyurl.4|
|[web, twitter, FB...| Denny| 7659| 3| Lee| 6/7/2019|https://tinyurl.3|
| [twitter, LinkedIn]| Brooke| 8908| 2| Wenig| 5/5/2018|https://tinyurl.2|
| [twitter, LinkedIn]| Jules| 4535| 1| Damji| 1/4/2016|https://tinyurl.1|
+--------------------+---------+-----+---+-------+---------+-----------------+

// COMMAND ----------

// MAGIC %md
// MAGIC ###Rows

// COMMAND ----------

// In Scala
import org.apache.spark.sql.Row
// Create a Row
val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
 Array("twitter", "LinkedIn"))
// Access using index for individual items
blogRow(1)

// COMMAND ----------

// In Scala
val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
val authorsDF = rows.toDF("Author", "State")
authorsDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ###Common DataFrame Operations
// MAGIC To perform common data operations on DataFrames, you’ll first need to load a DataFrame from a data source that holds your structured data. Spark provides an interface, DataFrameReader, that enables you to read data into a DataFrame from myriad data sources in formats such as JSON, CSV, Parquet, Text, Avro, ORC, etc. Likewise,
// MAGIC to write a DataFrame back to a data source in a particular format, Spark uses DataFrameWriter

// COMMAND ----------

// MAGIC %md
// MAGIC ####Using DataFrameReader and DataFrameWriter
// MAGIC we will define a schema for this file and use
// MAGIC the DataFrameReader class and its methods to tell Spark what to do. Because this file
// MAGIC contains 28 columns and over 4,380,660 records,2
// MAGIC  it’s more efficient to define a
// MAGIC schema than have Spark infer it.

// COMMAND ----------

// In Scala it would be similar
val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
StructField("UnitID", StringType, true),
 StructField("IncidentNumber", IntegerType, true),
 StructField("CallType", StringType, true),
 StructField("Location", StringType, true),
 ...
 ...
 StructField("Delay", FloatType, true)))
// Read the file using the CSV DataFrameReader
val sfFireFile="/FileStore/tables/sf_fire_calls-1.csv"
val fireDF = spark.read.schema(fireSchema)
 .option("header", "true")
 .csv(sfFireFile)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Saving a DataFrame as a Parquet file or SQL table
// MAGIC To write the DataFrame into an external data source in your format of choice, you
// MAGIC can use the DataFrameWriter interface. Like DataFrameReader, it supports multiple
// MAGIC data sources. 
// MAGIC 
// MAGIC Parquet, a popular columnar format, is the default format; it uses
// MAGIC snappy compression to compress the data. If the DataFrame is written as Parquet, the
// MAGIC schema is preserved as part of the Parquet metadata.  In this case, subsequent reads
// MAGIC back into a DataFrame do not require you to manually supply a schema

// COMMAND ----------

// MAGIC %md
// MAGIC  to persist the DataFrame we were just working with as a file after reading it you
// MAGIC would do the following:

// COMMAND ----------

// In Scala to save as a Parquet file
val parquetPath = ...
fireDF.write.format("parquet").save(parquetPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Alternatively, you can save it as a table, which registers metadata with the Hive metastore 

// COMMAND ----------

// In Scala to save as a table 
val parquetTable = ... // name of the table
fireDF.write.format("parquet").saveAsTable(parquetTable)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Projections and filters
// MAGIC In Spark, projections are
// MAGIC done with the select() method, while filters can be expressed using the filter() or
// MAGIC where() method.

// COMMAND ----------

// In Scala
val fewFireDF = fireDF
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where($"CallType" =!= "Medical Incident") 
fewFireDF.show(5, false)


// COMMAND ----------

// MAGIC %md
// MAGIC What if we want to know how many distinct CallTypes were recorded as the causes
// MAGIC of the fire calls? These simple and expressive queries do the job:

// COMMAND ----------

// In Scala
import org.apache.spark.sql.functions._
fireDF
.select("CallType")
.where(col("CallType").isNotNull)
.agg(countDistinct('CallType) as 'DistinctCallTypes)
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC We can list the distinct call types in the data set using these queries:

// COMMAND ----------

// In Scala
fireDF
 .select("CallType")
 .where($"CallType".isNotNull())
 .distinct()
 .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Renaming, adding, and dropping columns.
// MAGIC  Spaces in column names can be problematic, especially when you want to write or save a
// MAGIC DataFrame as a Parquet file (which prohibits this). Alternatively, you could selectively rename columns with the withColumnRenamed()
// MAGIC method. For instance, let’s change the name of our Delay column to ResponseDe
// MAGIC layedinMins and take a look at the response times that were longer than five
// MAGIC minutes:

// COMMAND ----------

// In Scala
val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF
 .select("ResponseDelayedinMins")
 .where($"ResponseDelayedinMins" > 5)
 .show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Because DataFrame transformations are immutable, when we
// MAGIC rename a column using withColumnRenamed() we get a new Data‐Frame while retaining the original with the old column name. Modifying the contents of a column or its type are common operations during data
// MAGIC exploration. In some cases the data is raw or dirty, or its types are not amenable(malleable)

// COMMAND ----------

// MAGIC %md
// MAGIC the columns CallDate, WatchDate, and AlarmDtTm are strings
// MAGIC rather than either Unix timestamps or SQL dates. (During a date- or time based analysis of the data). It’s quite simple, thanks to
// MAGIC some high-level API methods. spark.sql.functions has a set of to/from date/time‐
// MAGIC stamp functions such as to_timestamp() and to_date()

// COMMAND ----------

// In Scala
val fireTsDF = newFireDF
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm")
// Select the converted columns
fireTsDF
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have modified the dates, we can query using functions from
// MAGIC spark.sql.functions like month(), year(), and day(). 

// COMMAND ----------

// In Scala
fireTsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####Aggregations
// MAGIC A handful of transformations and actions on DataFrames, such as groupBy(),
// MAGIC orderBy(), and count(), offer the ability to aggregate by column names and then
// MAGIC aggregate counts across them.

// COMMAND ----------

// In Scala 
fireTsDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC The DataFrame API also offers the collect() method, but for
// MAGIC extremely large DataFrames this is resource-heavy (expensive) and
// MAGIC dangerous, as it can cause out-of-memory (OOM) exceptions. Unlike count(), which returns a single number to the driver.
// MAGIC 
// MAGIC  If you want to take a peek at some Row records
// MAGIC you’re better off with take(n), which will return only the first n
// MAGIC Row objects of the DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC ####Other common DataFrame operations
// MAGIC the DataFrame API provides descriptive statistical methods like min(), max(), sum().
// MAGIC 
// MAGIC Here we compute the sum of alarms, the average response time, and the minimum
// MAGIC and maximum response times to all fire calls in our data set:

// COMMAND ----------

// In Scala
import org.apache.spark.sql.{functions => F}
fireTsDF
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##The Dataset API
// MAGIC Tanto un dataset como un dataframe son conjuntos de datos organizados en estructuras rectangulares en forma de tabla o matriz, que almacenan sus datos en filas y columnas y  con unas variables que corresponden a unos objetos.
// MAGIC 
// MAGIC Lo que diferencia a un dataframe de un dataset es que un dataframe es un dataset que a la vez está organizado en columnas, de modo que en el dataframe tendremos los datos estructurados y cada columna con su nombre correspondiente.
// MAGIC 
// MAGIC Las matrices almacenan un único tipo de datos pero en las matrices del dataframe se aceptan valores alfanuméricos por lo que otra característica que les diferencia es que el dataframe puede contener distintos tipos de datos.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Typed Objects, Untyped Objects, and Generic Rows
// MAGIC Spark 2.0 unified the DataFrame and Dataset APIs as
// MAGIC Structured APIs with similar interfaces so that developers would only have to learn a
// MAGIC single set of APIs.
// MAGIC 
// MAGIC DataFrame in Scala as an alias for a collection of
// MAGIC generic objects, Dataset[Row], where a Row is a generic untyped JVM object that may
// MAGIC hold different types of fields. 
// MAGIC 
// MAGIC A Dataset, by contrast, is a collection of strongly typed
// MAGIC JVM objects in Scala or a class in Java.

// COMMAND ----------

// In Scala
import org.apache.spark.sql.Row
val row = Row(350, true, "Learning Spark 2E", null)

row.getInt(0)

row.getBoolean(1)

row.getString(2)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Creating Datasets
// MAGIC when creating a Dataset you have to
// MAGIC know the schema. In other words, you need to know the data types. . Although with
// MAGIC JSON and CSV data it’s possible to infer the schema, for large data sets this is
// MAGIC resource-intensive (expensive).  When creating a Dataset in Scala, the easiest way to
// MAGIC specify the schema for the resulting Dataset is to use a case class. 

// COMMAND ----------

// In Scala
case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

val ds = spark.read
.json("/FileStore/tables/iot_devices.json")
.as[DeviceIoTData]
//ds: org.apache.spark.sql.Dataset[DeviceIoTData]
ds.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Dataset Operations
// MAGIC Just as you can perform transformations and actions on DataFrames, so you can with
// MAGIC Datasets. Depending on the kind of operation, the results will vary:

// COMMAND ----------

// In Scala
val filterTempDS = ds.filter(d => d.temp > 30 && d.humidity > 70)
filterTempDS: org.apache.spark.sql.Dataset[DeviceIoTData]
filterTempDS.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC other example:

// COMMAND ----------

// In Scala
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
 cca3: String)
val dsTemp = ds
 .filter(d => {d.temp > 25})
 .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
 .toDF("temp", "device_name", "device_id", "cca3")
.as[DeviceTempByCountry]
dsTemp.show(5, false)

// COMMAND ----------

// In Scala
val dsTemp2 = ds
 .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
 .where("temp > 25")
 .as[DeviceTempByCountry]
dsTemp2.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC you can inspect only the first row of your Dataset

// COMMAND ----------

val device = dsTemp.first()
println(device)

// COMMAND ----------

// MAGIC %md
// MAGIC ##DataFrames Versus Datasets
// MAGIC By now you may be wondering why and when you should use DataFrames or Data‐
// MAGIC sets. In many cases either will do, depending on the languages you are working in, but
// MAGIC there are some situations where one is preferable to the other. Here are a few
// MAGIC examples:
// MAGIC 
// MAGIC Both
// MAGIC 
// MAGIC • If you want to tell Spark what to do, not how to do it, use DataFrames or Datasets.
// MAGIC 
// MAGIC • If you want rich semantics, high-level abstractions, and DSL operators, use DataFrames or Datasets.
// MAGIC 
// MAGIC • If your processing demands high-level expressions, filters, maps, aggregations,
// MAGIC computing averages or sums, SQL queries, columnar access, or use of relational
// MAGIC operators on semi-structured data, use DataFrames or Datasets.
// MAGIC 
// MAGIC Dataframe
// MAGIC 
// MAGIC • If your processing dictates relational transformations similar to SQL-like queries,
// MAGIC use DataFrames.
// MAGIC 
// MAGIC • If you want unification, code optimization, and simplification of APIs across
// MAGIC Spark components, use DataFrames.
// MAGIC 
// MAGIC • If you are an R user, use DataFrames.
// MAGIC 
// MAGIC • If you are a Python user, use DataFrames and drop down to RDDs if you need
// MAGIC more control.
// MAGIC 
// MAGIC • If you want space and speed efficiency, use DataFrames.
// MAGIC 
// MAGIC 
// MAGIC dataset
// MAGIC 
// MAGIC • If you want strict compile-time type safety and don’t mind creating multiple case
// MAGIC classes for a specific Dataset[T], use Datasets.
// MAGIC 
// MAGIC • If you want to take advantage of and benefit from Tungsten’s efficient serialization with Encoders, use Datasets.

// COMMAND ----------

// MAGIC %md
// MAGIC ###When to Use RDDs
// MAGIC There are some scenarios where you’ll want to consider using RDDs, such as when
// MAGIC you:
// MAGIC 
// MAGIC • Are using a third-party package that’s written using RDDs
// MAGIC 
// MAGIC • Can forgo the code optimization, efficient space utilization, and performance
// MAGIC benefits available with DataFrames and Datasets
// MAGIC 
// MAGIC • Want to precisely instruct Spark how to do a query
// MAGIC 
// MAGIC You can seamlessly move between DataFrames or Datasets and RDDs at
// MAGIC will using a simple API method call, df.rdd. (Note, however, that this does have a
// MAGIC cost and should be avoided unless necessary.)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Spark SQL and the Underlying Engine

// COMMAND ----------

// MAGIC %md
// MAGIC ###The Catalyst Optimizer
// MAGIC The Catalyst optimizer takes a computational query and converts it into an execution
// MAGIC plan. It goes through four transformational phases
// MAGIC 
// MAGIC - Analysis
// MAGIC 
// MAGIC In this initial phase, any columns or table names will be resolved
// MAGIC by consulting an internal Catalog, a programmatic interface to Spark SQL that holds
// MAGIC a list of names of columns, data types, functions, tables, databases, etc. Once they’ve
// MAGIC all been successfully resolved, the query proceeds to the next phase
// MAGIC 
// MAGIC - Logical optimization
// MAGIC 
// MAGIC Applying a standardrule based optimization approach, the Catalyst optimizer will first construct a set of multiple plans and then, using its cost-based optimizer (CBO), assign costs to each plan.
// MAGIC 
// MAGIC - Physical planning
// MAGIC 
// MAGIC In this phase, Spark SQL generates an optimal physical plan for the selected logical
// MAGIC plan, using physical operators that match those available in the Spark execution engine.
// MAGIC 
// MAGIC 
// MAGIC - Code generation
// MAGIC 
// MAGIC The final phase of query optimization involves generating efficient Java bytecode to run on each machine. In other words, it acts as a compiler. Project Tungsten, which facilitates whole-stage code generation, plays a role here. Just what is whole-stage code generation? It’s a physical query optimization phase that collapses the whole query into a single function, getting rid of virtual function calls and employing CPU registers for intermediate data.  uses this approach to generate compact RDD code for final execution. This streamlined strategy significantly improves CPU efficiency and performance.