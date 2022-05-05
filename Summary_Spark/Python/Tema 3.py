# Databricks notebook source
# MAGIC %md
# MAGIC #Tema 3 del libro - Structured APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark: What’s Underneath an RDD?
# MAGIC The RDD is the most basic abstraction in Spark. There are three vital characteristics
# MAGIC associated with an RDD:
# MAGIC 
# MAGIC • Dependencies
# MAGIC 
# MAGIC • Partitions (with some locality information)
# MAGIC 
# MAGIC • Compute function: Partition => Iterator[T]
# MAGIC 
# MAGIC First, a list of dependencies that instructs
# MAGIC Spark how an RDD is constructed with its inputs is required. When necessary to
# MAGIC reproduce results, Spark can recreate an RDD from these dependencies and replicate
# MAGIC operations on it. This characteristic gives RDDs resiliency.
# MAGIC 
# MAGIC Second, partitions provide Spark the ability to split the work to parallelize computation on partitions across executors. In some cases—for example, reading from HDFS—Spark will use locality information to send work to executors close to the data. That way less data is transmitted over the network.
# MAGIC 
# MAGIC And finally, an RDD has a compute function that produces an Iterator[T] for the
# MAGIC data that will be stored in the RDD

# COMMAND ----------

# MAGIC %md
# MAGIC ##Structuring Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ###Key Merits and Benefits
# MAGIC Structure yields a number of benefits, including better performance and space effi‐
# MAGIC ciency across Spark components. Like DataFrame and dataSet, but there are other advantages: expressivity, simplicity, composability, and uniformity.
# MAGIC 
# MAGIC the following example, we want to aggregate all the ages for each name, group by
# MAGIC name, and then average the ages. If we were to use the low-level RDD API for this, the code would look as follows:

# COMMAND ----------

# MAGIC %md
# MAGIC the code is instructing Spark how to compute the query. It’s completely
# MAGIC opaque to Spark, because it doesn’t communicate the intention

# COMMAND ----------

# In Python
# Create an RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30),
 ("TD", 35), ("Brooke", 25)])
# Use map and reduceByKey transformations with their lambda 
# expressions to aggregate and then compute average
agesRDD = (dataRDD
 .map(lambda x: (x[0], (x[1], 1)))
 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
 .map(lambda x: (x[0], x[1][0]/x[1][1])))

# COMMAND ----------

# MAGIC %md
# MAGIC It's the same query with high-level DSL operators
# MAGIC and the DataFrame API

# COMMAND ----------

# In Python 
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Create a DataFrame using SparkSession
spark = (SparkSession
 .builder
 .appName("AuthorsAges")
 .getOrCreate())
# Create a DataFrame 
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
                                 ("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC This version of the code is far more expressive as well as simpler than the earlier version, because we are using high-level DSL operators and APIs to tell Spark what to
# MAGIC do. Spark can inspect or parse this query and understand our intention, it can optimize
# MAGIC or arrange the operations for efficient execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ##The DataFrame API
# MAGIC Inspired by pandas DataFrames in structure, format, and a few specific operations,
# MAGIC Spark DataFrames are like distributed in-memory tables with named columns and
# MAGIC schemas. When data is visualized as a structured table, it’s not only easy to digest but also easy
# MAGIC to work with when it comes to common operations you might want to execute on
# MAGIC rows and columns.
# MAGIC 
# MAGIC You can add or change
# MAGIC the names and data types of the columns, creating new DataFrames while the previous versions are preserved.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Spark’s Basic Data Types
# MAGIC Spark supports basic internal data
# MAGIC types. These data types can be declared in your Spark application or defined in your
# MAGIC schema. For example, in Scala, you can define or declare a particular column name to
# MAGIC be of type String, Byte, Long, or Map, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC Spark supports similar basic Python data types

# COMMAND ----------

# MAGIC %md
# MAGIC ###Spark’s Structured and Complex Data Types
# MAGIC For complex data analytics, you won’t deal only with simple or basic data types. Your
# MAGIC data will be complex.They come in many forms: maps, arrays, structs, dates,
# MAGIC timestamps, fields, etc. 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schemas and Creating DataFrames
# MAGIC Defining a schema
# MAGIC up front as opposed to taking a schema-on-read approach offers three benefits:
# MAGIC 
# MAGIC • You relieve Spark from the onus of inferring data types.
# MAGIC 
# MAGIC • You prevent Spark from creating a separate job just to read a large portion of your file to ascertain the schema, which for a large data file can be expensive and time-consuming.
# MAGIC 
# MAGIC • You can detect errors early if data doesn’t match the schema.
# MAGIC 
# MAGIC you to always define your schema up front whenever you want to
# MAGIC read a large file from a data source.

# COMMAND ----------

# MAGIC %md
# MAGIC Spark allows you to define a schema in two ways. One is to define it programmatically, and the other is to employ a Data Definition Language (DDL) string, which is
# MAGIC much simpler and easier to read.

# COMMAND ----------

# MAGIC %md
# MAGIC To define a schema programmatically for a DataFrame with three named columns,
# MAGIC author, title, and pages, you can use the Spark DataFrame API. For example:

# COMMAND ----------

# In Python
from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
 StructField("title", StringType(), False),
 StructField("pages", IntegerType(), False)])

# COMMAND ----------

# MAGIC %md
# MAGIC Defining the same schema using DDL is much simpler:

# COMMAND ----------

# In Python
schema = "author STRING, title STRING, pages INT"

# COMMAND ----------

# MAGIC %md
# MAGIC For many examples, we
# MAGIC will use both:

# COMMAND ----------

# In Python 
from pyspark.sql import SparkSession
# Define schema for our data using DDL 
schema = "'Id' INT, 'First' STRING, 'Last' STRING, 'Url' STRING, 'Published' STRING, 'Hits' INT, 'Campaigns' ARRAY<STRING>"
# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
        [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter","LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web","twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,["twitter", "FB"]],
        [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web","twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,["twitter", "LinkedIn"]]]

# Main program
if __name__ == "__main__":
    # Create a SparkSession
    spark = (SparkSession.builder.appName("Example-3_6").getOrCreate())
    # Create a DataFrame using the schema defined above
    blogs_df = spark.createDataFrame(data,schema)
    # Show the DataFrame; it should reflect our table above
    blogs_df.show()
    # Print the schema used by Spark to process the DataFrame
    print(blogs_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rows

# COMMAND ----------

# In Python
from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
 ["twitter", "LinkedIn"])
# access using index for individual items
blog_row[1]

# COMMAND ----------

# In Python 
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Common DataFrame Operations
# MAGIC To perform common data operations on DataFrames, you’ll first need to load a DataFrame from a data source that holds your structured data. Spark provides an interface, DataFrameReader, that enables you to read data into a DataFrame from myriad data sources in formats such as JSON, CSV, Parquet, Text, Avro, ORC, etc. Likewise,
# MAGIC to write a DataFrame back to a data source in a particular format, Spark uses DataFrameWriter

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using DataFrameReader and DataFrameWriter
# MAGIC we will define a schema for this file and use
# MAGIC the DataFrameReader class and its methods to tell Spark what to do. Because this file
# MAGIC contains 28 columns and over 4,380,660 records,2
# MAGIC  it’s more efficient to define a
# MAGIC schema than have Spark infer it.

# COMMAND ----------

# In Python, define a schema 
from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Saving a DataFrame as a Parquet file or SQL table
# MAGIC To write the DataFrame into an external data source in your format of choice, you
# MAGIC can use the DataFrameWriter interface. Like DataFrameReader, it supports multiple
# MAGIC data sources. 
# MAGIC 
# MAGIC Parquet, a popular columnar format, is the default format; it uses
# MAGIC snappy compression to compress the data. If the DataFrame is written as Parquet, the
# MAGIC schema is preserved as part of the Parquet metadata.  In this case, subsequent reads
# MAGIC back into a DataFrame do not require you to manually supply a schema

# COMMAND ----------

# MAGIC %md
# MAGIC  to persist the DataFrame we were just working with as a file after reading it you
# MAGIC would do the following:

# COMMAND ----------

# In Python, define a schema 
from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
# In Python to save as a Parquet file
parquet_path = "/FileStore/tables/prueba.txt"
fire_df.write.format("parquet").save(parquet_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, you can save it as a table, which registers metadata with the Hive metastore 

# COMMAND ----------

# In Python
parquet_table = ... # name of the table
fire_df.write.format("parquet").saveAsTable(parquet_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Projections and filters
# MAGIC In Spark, projections are
# MAGIC done with the select() method, while filters can be expressed using the filter() or
# MAGIC where() method.

# COMMAND ----------

from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
few_fire_df = (fire_df
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC What if we want to know how many distinct CallTypes were recorded as the causes
# MAGIC of the fire calls? These simple and expressive queries do the job:

# COMMAND ----------

from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
# In Python, return number of distinct types of calls using countDistinct()
from pyspark.sql.functions import *
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC We can list the distinct call types in the data set using these queries:

# COMMAND ----------

from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
# In Python, filter for only distinct non-null CallTypes from all the rows
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming, adding, and dropping columns.
# MAGIC  Spaces in column names can be problematic, especially when you want to write or save a
# MAGIC DataFrame as a Parquet file (which prohibits this). Alternatively, you could selectively rename columns with the withColumnRenamed()
# MAGIC method. For instance, let’s change the name of our Delay column to ResponseDe
# MAGIC layedinMins and take a look at the response times that were longer than five
# MAGIC minutes:

# COMMAND ----------

from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

# COMMAND ----------

# MAGIC %md
# MAGIC Because DataFrame transformations are immutable, when we
# MAGIC rename a column using withColumnRenamed() we get a new Data‐Frame while retaining the original with the old column name. Modifying the contents of a column or its type are common operations during data
# MAGIC exploration. In some cases the data is raw or dirty, or its types are not amenable(malleable)

# COMMAND ----------

# MAGIC %md
# MAGIC the columns CallDate, WatchDate, and AlarmDtTm are strings
# MAGIC rather than either Unix timestamps or SQL dates. (During a date- or time based analysis of the data). It’s quite simple, thanks to
# MAGIC some high-level API methods. spark.sql.functions has a set of to/from date/time‐
# MAGIC stamp functions such as to_timestamp() and to_date()

# COMMAND ----------

# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True), 
                          StructField('CallDate', StringType(), True), 
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True), 
                          StructField('City', StringType(), True), 
                          StructField('Zipcode', IntegerType(), True), 
                          StructField('Battalion', StringType(), True), 
                          StructField('StationArea', StringType(), True), 
                          StructField('Box', StringType(), True), 
                          StructField('OriginalPriority', StringType(), True), 
                          StructField('Priority', StringType(), True), 
                          StructField('FinalPriority', IntegerType(), True), 
                          StructField('ALSUnit', BooleanType(), True), 
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/sf_fire_calls-1.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
fire_ts_df = (new_fire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have modified the dates, we can query using functions from
# MAGIC spark.sql.functions like month(), year(), and day(). 

# COMMAND ----------

(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Aggregations
# MAGIC A handful of transformations and actions on DataFrames, such as groupBy(),
# MAGIC orderBy(), and count(), offer the ability to aggregate by column names and then
# MAGIC aggregate counts across them.

# COMMAND ----------

(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC The DataFrame API also offers the collect() method, but for
# MAGIC extremely large DataFrames this is resource-heavy (expensive) and
# MAGIC dangerous, as it can cause out-of-memory (OOM) exceptions. Unlike count(), which returns a single number to the driver.
# MAGIC 
# MAGIC  If you want to take a peek at some Row records
# MAGIC you’re better off with take(n), which will return only the first n
# MAGIC Row objects of the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Other common DataFrame operations
# MAGIC the DataFrame API provides descriptive statistical methods like min(), max(), sum().
# MAGIC 
# MAGIC Here we compute the sum of alarms, the average response time, and the minimum
# MAGIC and maximum response times to all fire calls in our data set:

# COMMAND ----------

# In Python
import pyspark.sql.functions as F
(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
         F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ##The Dataset API
# MAGIC Tanto un dataset como un dataframe son conjuntos de datos organizados en estructuras rectangulares en forma de tabla o matriz, que almacenan sus datos en filas y columnas y  con unas variables que corresponden a unos objetos.
# MAGIC 
# MAGIC Lo que diferencia a un dataframe de un dataset es que un dataframe es un dataset que a la vez está organizado en columnas, de modo que en el dataframe tendremos los datos estructurados y cada columna con su nombre correspondiente.
# MAGIC 
# MAGIC Las matrices almacenan un único tipo de datos pero en las matrices del dataframe se aceptan valores alfanuméricos por lo que otra característica que les diferencia es que el dataframe puede contener distintos tipos de datos.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Typed Objects, Untyped Objects, and Generic Rows
# MAGIC Spark 2.0 unified the DataFrame and Dataset APIs as
# MAGIC Structured APIs with similar interfaces so that developers would only have to learn a
# MAGIC single set of APIs.
# MAGIC 
# MAGIC DataFrame in Scala as an alias for a collection of
# MAGIC generic objects, Dataset[Row], where a Row is a generic untyped JVM object that may
# MAGIC hold different types of fields. 
# MAGIC 
# MAGIC A Dataset, by contrast, is a collection of strongly typed
# MAGIC JVM objects in Scala or a class in Java.

# COMMAND ----------

# In Python
from pyspark.sql import Row
row = Row(350, True, "Learning Spark 2E", None)

row[0]

row[1]

row[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating Datasets
# MAGIC when creating a Dataset you have to
# MAGIC know the schema. In other words, you need to know the data types. . Although with
# MAGIC JSON and CSV data it’s possible to infer the schema, for large data sets this is
# MAGIC resource-intensive (expensive).  When creating a Dataset in Scala, the easiest way to
# MAGIC specify the schema for the resulting Dataset is to use a case class. 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dataset Operations
# MAGIC Just as you can perform transformations and actions on DataFrames, so you can with
# MAGIC Datasets. Depending on the kind of operation, the results will vary. Also, you can inspect only the first row of your Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ##DataFrames Versus Datasets
# MAGIC By now you may be wondering why and when you should use DataFrames or Data‐
# MAGIC sets. In many cases either will do, depending on the languages you are working in, but
# MAGIC there are some situations where one is preferable to the other. Here are a few
# MAGIC examples:
# MAGIC 
# MAGIC Both
# MAGIC 
# MAGIC • If you want to tell Spark what to do, not how to do it, use DataFrames or Datasets.
# MAGIC 
# MAGIC • If you want rich semantics, high-level abstractions, and DSL operators, use DataFrames or Datasets.
# MAGIC 
# MAGIC • If your processing demands high-level expressions, filters, maps, aggregations,
# MAGIC computing averages or sums, SQL queries, columnar access, or use of relational
# MAGIC operators on semi-structured data, use DataFrames or Datasets.
# MAGIC 
# MAGIC Dataframe
# MAGIC 
# MAGIC • If your processing dictates relational transformations similar to SQL-like queries,
# MAGIC use DataFrames.
# MAGIC 
# MAGIC • If you want unification, code optimization, and simplification of APIs across
# MAGIC Spark components, use DataFrames.
# MAGIC 
# MAGIC • If you are an R user, use DataFrames.
# MAGIC 
# MAGIC • If you are a Python user, use DataFrames and drop down to RDDs if you need
# MAGIC more control.
# MAGIC 
# MAGIC • If you want space and speed efficiency, use DataFrames.
# MAGIC 
# MAGIC 
# MAGIC dataset
# MAGIC 
# MAGIC • If you want strict compile-time type safety and don’t mind creating multiple case
# MAGIC classes for a specific Dataset[T], use Datasets.
# MAGIC 
# MAGIC • If you want to take advantage of and benefit from Tungsten’s efficient serialization with Encoders, use Datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ###When to Use RDDs
# MAGIC There are some scenarios where you’ll want to consider using RDDs, such as when
# MAGIC you:
# MAGIC 
# MAGIC • Are using a third-party package that’s written using RDDs
# MAGIC 
# MAGIC • Can forgo the code optimization, efficient space utilization, and performance
# MAGIC benefits available with DataFrames and Datasets
# MAGIC 
# MAGIC • Want to precisely instruct Spark how to do a query
# MAGIC 
# MAGIC You can seamlessly move between DataFrames or Datasets and RDDs at
# MAGIC will using a simple API method call, df.rdd. (Note, however, that this does have a
# MAGIC cost and should be avoided unless necessary.)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark SQL and the Underlying Engine

# COMMAND ----------

# MAGIC %md
# MAGIC ###The Catalyst Optimizer
# MAGIC The Catalyst optimizer takes a computational query and converts it into an execution
# MAGIC plan. It goes through four transformational phases
# MAGIC 
# MAGIC - Analysis
# MAGIC 
# MAGIC In this initial phase, any columns or table names will be resolved
# MAGIC by consulting an internal Catalog, a programmatic interface to Spark SQL that holds
# MAGIC a list of names of columns, data types, functions, tables, databases, etc. Once they’ve
# MAGIC all been successfully resolved, the query proceeds to the next phase
# MAGIC 
# MAGIC - Logical optimization
# MAGIC 
# MAGIC Applying a standardrule based optimization approach, the Catalyst optimizer will first construct a set of multiple plans and then, using its cost-based optimizer (CBO), assign costs to each plan.
# MAGIC 
# MAGIC - Physical planning
# MAGIC 
# MAGIC In this phase, Spark SQL generates an optimal physical plan for the selected logical
# MAGIC plan, using physical operators that match those available in the Spark execution engine.
# MAGIC 
# MAGIC 
# MAGIC - Code generation
# MAGIC 
# MAGIC The final phase of query optimization involves generating efficient Java bytecode to run on each machine. In other words, it acts as a compiler. Project Tungsten, which facilitates whole-stage code generation, plays a role here. Just what is whole-stage code generation? It’s a physical query optimization phase that collapses the whole query into a single function, getting rid of virtual function calls and employing CPU registers for intermediate data.  uses this approach to generate compact RDD code for final execution. This streamlined strategy significantly improves CPU efficiency and performance.
# MAGIC 
# MAGIC That is, regardless of the language you use, your computation undergoes the same journey and the resulting bytecode is likely the same:

# COMMAND ----------

# In Python
count_mnm_df = (mnm_df
 .select("State", "Color", "Count")
 .groupBy("State", "Color")
 .agg(count("Count")
 .alias("Total"))
 .orderBy("Total", ascending=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC SELECT State, Color, Count, sum(Count) AS Total
# MAGIC FROM MNM_TABLE_NAME
# MAGIC GROUP BY State, Color, Count
# MAGIC ORDER BY Total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC To see the different stages the Python code goes through, you can use the
# MAGIC count_mnm_df.explain(True) method on the DataFrame.

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
if __name__ == "__main__":

    spark = (SparkSession
    .builder
    .appName("PythonMnMCount")
    .getOrCreate())

    mnm_df=(spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("/FileStore/tables/mnm_dataset.csv"))

    count_mnm_df = (mnm_df
    .select("State","Color","Count")
    .groupBy("State","Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))

    #count_mnm_df.show(n=60,truncate=False)

    #print("Total Rows = %d" % (count_mnm_df.count()))

    ca_count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))

    #ca_count_mnm_df.show(n=10, truncate=False)
    count_mnm_df.explain(True)