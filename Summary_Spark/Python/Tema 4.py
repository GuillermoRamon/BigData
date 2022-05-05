# Databricks notebook source
# MAGIC %md
# MAGIC #Tema 4 - Spark SQL and DataFrames: Introduction to Built-in Data Sources

# COMMAND ----------

# MAGIC %md
# MAGIC ##Using Spark SQL in Spark Applications

# COMMAND ----------

# MAGIC %md
# MAGIC ###Basic Query Examples (SQL)
# MAGIC 
# MAGIC In this section we’ll walk through a few examples of queries on the Airline On-Time
# MAGIC Performance and Causes of Flight Delays data set, which contains data on US flights
# MAGIC including date, delay, distance, origin, and destination. 
# MAGIC 
# MAGIC It’s available as a CSV file with
# MAGIC over a million records. Using a schema, we’ll read the data into a DataFrame and register the DataFrame as a temporary view (more on temporary views shortly) so we
# MAGIC can query it with SQL

# COMMAND ----------

# Path to data set
csv_file = "/FileStore/tables/departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you 
# may want to specify the schema)
df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# In Python
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)
# Or
(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False).show(10))


# COMMAND ----------

# If you want to specify a schema, you can use a DDL-formatted string.
schema = "`date` STRING, `delay` INT, `distance` INT, 
`origin` STRING, `destination` STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC <a href="$../Summary_Spark/Ejercicios">Link de los ejercicios del tema</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##SQL Tables and Views
# MAGIC Tables hold data. Associated with each table in Spark is its relevant metadata, which is information about the table and its data: the schema, description, table name, data‐base name, column names, partitions, physical location where the actual data resides,
# MAGIC etc. All of this is stored in a central metastore.
# MAGIC 
# MAGIC  Spark by default uses the Apache Hive metastore, located at /user/hive/warehouse, to persist all the metadata about your tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Managed Versus UnmanagedTables
# MAGIC 
# MAGIC Spark allows you to create two types of tables: managed and unmanaged.
# MAGIC 
# MAGIC - For a managed table, Spark manages both the metadata and the data in the file store. This could be a local filesystem, HDFS, or an object store such as Amazon S3 or Azure Blob.
# MAGIC 
# MAGIC - For an unmanaged table, Spark only manages the metadata, while you manage the data
# MAGIC yourself in an external data source such as Cassandra
# MAGIC 
# MAGIC With a managed table, because Spark manages everything, a SQL command such as
# MAGIC DROP TABLE table_name deletes both the metadata and the data. With an unmanaged
# MAGIC table, the same command will delete only the metadata, not the actual data. We will
# MAGIC look at some examples of how to create managed and unmanaged tables in the next
# MAGIC section

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating SQL Databases and Tables
# MAGIC 
# MAGIC Tables reside within a database. By default, Spark creates tables under the default
# MAGIC database. To create your own database name, you can issue a SQL command from
# MAGIC your Spark application or notebook
# MAGIC let’s create both a managed and an unmanaged table.

# COMMAND ----------

# MAGIC %md
# MAGIC From this point, any commands we issue in our application to create tables will result
# MAGIC in the tables being created in this database and residing under the database name
# MAGIC learn_spark_db

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a managed table
# MAGIC To create a managed table within the database learn_spark_db, you can issue a SQL
# MAGIC query like the following:

# COMMAND ----------

spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# COMMAND ----------

# MAGIC %md
# MAGIC You can do the same thing using the DataFrame API like this:

# COMMAND ----------

# In Python
# Path to our US flight delays CSV file 
csv_file = "/FileStore/tables/departuredelays.csv"
# Schema as defined in the preceding example
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating an unmanaged table
# MAGIC By contrast, you can create unmanaged tables from your own data sources—say, Parquet, CSV, or JSON files stored in a file store accessible to your Spark application.

# COMMAND ----------

spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (PATH'/FileStore/tables/departuredelays.csv')""")

# COMMAND ----------

# MAGIC %md
# MAGIC And within the DataFrame API use

# COMMAND ----------

(flights_df
 .write
 .option("path", "/FileStore/tables/departuredelays.csv")
 .saveAsTable("us_delay_flights_tbl"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating Views
# MAGIC In addition to creating tables, Spark can create views on top of existing tables. Views
# MAGIC can be global (visible across all SparkSessions on a given cluster) or session-scoped
# MAGIC (visible only to a single SparkSession), and they are temporary: they disappear after
# MAGIC your Spark application terminates.
# MAGIC 
# MAGIC Creating views has a similar syntax to creating tables within a database. Once you create a view, you can query it as you would a table. 
# MAGIC 
# MAGIC The difference between a view and a
# MAGIC table is that views don’t actually hold the data; tables persist after your Spark application terminates, but views disappear.
# MAGIC 
# MAGIC You can create a view from an existing table using SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC  For example, if you wish to
# MAGIC work on only the subset of the US flight delays data set with origin airports of New
# MAGIC York (JFK) and San Francisco (SFO), the following queries will create global temporary and temporary views consisting of just that slice of the table:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
# MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
# MAGIC  origin = 'SFO';
# MAGIC CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
# MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
# MAGIC  origin = 'JFK'

# COMMAND ----------

# MAGIC %md
# MAGIC You can accomplish the same thing with the DataFrame API as follows:

# COMMAND ----------

# In Python
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM 
 us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM 
 us_delay_flights_tbl WHERE origin = 'JFK'")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")


# COMMAND ----------

# MAGIC %md
# MAGIC Once you’ve created these views, you can issue queries against them just as you would
# MAGIC against a table. Keep in mind that when accessing a global temporary view you must
# MAGIC use the prefix global_temp.<view_name>, because Spark creates global temporary
# MAGIC views in a global temporary database called global_temp. For example:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC By contrast, you can access the normal temporary view without the global_temp
# MAGIC prefix:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

# COMMAND ----------

# In Scala/Python
spark.read.table("us_origin_airport_JFK_tmp_view")
# Or
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC You can also drop a view just like you would a table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
# MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

# COMMAND ----------

# In Scala/Python
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Temporary views versus global temporary views
# MAGIC 
# MAGIC The difference between temporary and global temporary views being subtle, it can be a
# MAGIC source of mild confusion among developers new to Spark. A temporary view is tied
# MAGIC to a single SparkSession within a Spark application. In contrast, a global temporary
# MAGIC view is visible across multiple SparkSessions within a Spark application.
# MAGIC 
# MAGIC you can create multiple SparkSessions within a single Spark application. this can be handy,
# MAGIC for example, in cases where you want to access (and combine) data from two different
# MAGIC SparkSessions that don’t share the same Hive metastore configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Viewing the Metadata
# MAGIC As mentioned previously, Spark manages the metadata associated with each managed
# MAGIC or unmanaged table. This is captured in the Catalog, a high-level abstraction in
# MAGIC Spark SQL for storing metadata. There are methods enabling you to examine the metadata associated with
# MAGIC your databases, tables, and views. 
# MAGIC 
# MAGIC For example, within a Spark application, after creating the SparkSession variable
# MAGIC spark, you can access all the stored metadata through methods like these:

# COMMAND ----------

# In Scala/Python
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Caching SQL Tables
# MAGIC like DataFrames, you can cache and uncache SQL tables and views. you can specify a table as LAZY, meaning that it should only be cached when it is first used instead of immediately:

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE [LAZY] TABLE <table-name>
# MAGIC UNCACHE TABLE <table-name>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading Tables into DataFrames
# MAGIC Often, data engineers build data pipelines as part of their regular data ingestion and
# MAGIC ETL processes. They populate Spark SQL databases and tables with cleansed data for
# MAGIC consumption by applications downstream.
# MAGIC 
# MAGIC Let’s assume you have an existing database, learn_spark_db, and table,
# MAGIC us_delay_flights_tbl, ready for use. Instead of reading from an external JSON file,
# MAGIC you can simply use SQL to query the table and assign the returned result to a
# MAGIC DataFrame:

# COMMAND ----------

# MAGIC %python
# MAGIC # In Python
# MAGIC us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
# MAGIC us_flights_df2 = spark.table("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC Now you have a cleansed DataFrame read from an existing Spark SQL table. You can
# MAGIC also read data in other formats using Spark’s built-in data sources, giving you the flex‐
# MAGIC ibility to interact with various common file formats.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Sources for DataFrames and SQL Tables
# MAGIC Spark SQL provides an interface to a variety of data sources.
# MAGIC It also provides a set of common methods for reading and writing data to and from
# MAGIC these data sources using the Data Sources API.
# MAGIC 
# MAGIC first, let’s take a closer look at two high-level Data Source API constructs that dictate the manner in which you interact with different data sources: DataFrameReader and DataFrameWriter.

# COMMAND ----------

# MAGIC %md
# MAGIC ###DataFrameReader
# MAGIC DataFrameReader is the core construct for reading data from a data source into a
# MAGIC DataFrame. It has a defined format and a recommended pattern for usage:
# MAGIC 
# MAGIC *DataFrameReader.format(args).option("key", "value").schema(args).load()*
# MAGIC 
# MAGIC This pattern of stringing methods together is common in Spark, and easy to read.
# MAGIC Note that you can only access a DataFrameReader through a SparkSession instance.
# MAGIC 
# MAGIC While read returns a handle to DataFrameReader to read into a DataFrame from a
# MAGIC static data source, readStream returns an instance to read from a streaming source.
# MAGIC 
# MAGIC DataFrameReader methods, arguments, and options:
# MAGIC 
# MAGIC   - format()  
# MAGIC  
# MAGIC   "parquet", "csv", "txt", "json","jdbc", "orc", "avro", etc.
# MAGIC  
# MAGIC   If you don’t specify this method, then the default is Parquet or whatever is set in spark.sql.sources.default
# MAGIC   
# MAGIC   - option()
# MAGIC   
# MAGIC   A series of key/value pairs and options. The default mode is PERMISSIVE. The "inferSchema" and "mode" options are specific to the JSON and CSV file formats.
# MAGIC   
# MAGIC   - schema()
# MAGIC   
# MAGIC   For JSON or CSV format, you can specify to infer the schema in the option() method. Generally, providing a schema for any format makes loading faster and ensures your data conforms to the expected schema
# MAGIC   
# MAGIC   - load() 
# MAGIC   
# MAGIC   "/path/to/data/source"
# MAGIC   
# MAGIC   This can be empty if specified in option("path", "...").
# MAGIC   
# MAGIC   Example:
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC In general, no schema is needed when reading from a static Parquet
# MAGIC data source—the Parquet metadata usually contains the schema, so
# MAGIC it’s inferred. However, for streaming data sources you will have to
# MAGIC provide a schema. 
# MAGIC 
# MAGIC Parquet is the default and preferred data source for Spark because
# MAGIC it’s efficient

# COMMAND ----------

# MAGIC %md
# MAGIC ###DataFrameWriter
# MAGIC it saves or writes data to a specified built-in data source,
# MAGIC Unlike with DataFrameReader, you access its instance not from a SparkSession but from the DataFrame you wish to save.
# MAGIC  
# MAGIC  It has a few recommended usage patterns:
# MAGIC  
# MAGIC  *DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)*
# MAGIC  
# MAGIC  - format() 
# MAGIC  
# MAGIC  If you don’t specify this method, then the default is Parquet or whatever is set in spark.sql.sources.default.
# MAGIC 
# MAGIC  - option()
# MAGIC  
# MAGIC  A series of key/value pairs and options. This is an overloaded method. The default mode options are error or errorifexists and SaveMode.ErrorIfExists; they throw an exception at runtime if the data already exists.
# MAGIC  
# MAGIC  - bucketBy()
# MAGIC  
# MAGIC  The number of buckets and names of columns to bucket by. Uses Hive’s bucketing scheme on a filesystem.
# MAGIC  
# MAGIC  - save()
# MAGIC  
# MAGIC  "/path/to/data/source" 
# MAGIC  
# MAGIC  The path to save to. This can be empty if specified in option("path", "...").
# MAGIC 
# MAGIC  - saveAsTable()
# MAGIC  
# MAGIC  The table to save to.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parquet
# MAGIC  it’s the default data
# MAGIC source in Spark. Supported and widely used by many big data processing frameworks
# MAGIC and platforms, Parquet is an open source columnar file format that offers many I/O
# MAGIC optimizations (such as compression, which saves storage space and allows for quick
# MAGIC access to data columns).

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading Parquet files into a DataFrame
# MAGIC Parquet files are stored in a directory structure that contains the data files, metadata,
# MAGIC a number of compressed files, and some status files.
# MAGIC 
# MAGIC Metadata in the footer contains the version of the file format, the schema, and column data such as the path, etc.
# MAGIC For example, a directory in a Parquet file might contain a set of files like this:
# MAGIC - _SUCCESS
# MAGIC - _committed_1799640464332036264
# MAGIC - _started_1799640464332036264
# MAGIC - part-00000-tid-1799640464332036264-91273258-d7ef-4dc7-<...>-c000.snappy.parquet
# MAGIC 
# MAGIC To read Parquet files into a DataFrame, you simply specify the format and path:

# COMMAND ----------

# In Python
file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
 2010-summary.parquet/"""
df = spark.read.format("parquet").load(file)

# COMMAND ----------

# MAGIC %md
# MAGIC Unless you are reading from a streaming data source there’s no need to supply the
# MAGIC schema, because Parquet saves it as part of its metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading Parquet files into a Spark SQL table
# MAGIC As well as reading Parquet files into a Spark DataFrame, you can also create a Spark
# MAGIC SQL unmanaged table or view directly using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING parquet
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
# MAGIC  2010-summary.parquet/" )

# COMMAND ----------

# MAGIC %md
# MAGIC Once you’ve created the table or view, you can read data into a DataFrame using SQL,
# MAGIC as we saw in some earlier examples:

# COMMAND ----------

# In Python
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing DataFrames to Parquet files
# MAGIC Writing or saving a DataFrame as a table or file is a common operation in Spark.  If you don’t include
# MAGIC the format() method, the DataFrame will still be saved as a Parquet file.
# MAGIC Example:

# COMMAND ----------

# In Python
(df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet"))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing DataFrames to Spark SQL tables
# MAGIC Writing a DataFrame to a SQL table is as easy as writing to a file—just use saveAsTable() instead of save().
# MAGIC 
# MAGIC This will create a managed table called
# MAGIC us_delay_flights_tbl:

# COMMAND ----------

# In Python
(df.write
 .mode("overwrite")
 .saveAsTable("us_delay_flights_tbl"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###JSON
# MAGIC JavaScript Object Notation (JSON) is also a popular data format. It came to prominence as an easy-to-read and easy-to-parse format compared to XML. It has two representational formats: single-line mode and multiline mode. Both modes are supported in Spark.
# MAGIC 
# MAGIC In single-line mode each line denotes a single JSON object, whereas in multiline
# MAGIC mode the entire multiline object constitutes a single JSON object. To read in this
# MAGIC mode, set multiLine to true in the option() method.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading a JSON file into a DataFrame
# MAGIC  the same way you did with Parquet—just
# MAGIC specify "json" in the format() method:

# COMMAND ----------

# In Python
file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
df = spark.read.format("json").load(file)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading a JSON file into a Spark SQL table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING json
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
# MAGIC  )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing DataFrames to JSON files
# MAGIC Saving a DataFrame as a JSON file is simple. Specify the appropriate
# MAGIC DataFrameWriter methods and arguments, and supply the location to save the JSON
# MAGIC files to:

# COMMAND ----------

# In Python
(df.write.format("json")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/json/df_json"))


# COMMAND ----------

# MAGIC %md
# MAGIC ####JSON data source options
# MAGIC Common JSON options for DataFrameReader and DataFrame
# MAGIC Writer.
# MAGIC - compression
# MAGIC - dateFormat
# MAGIC - multiLine
# MAGIC - allowUnquotedFieldNames

# COMMAND ----------

# MAGIC %md
# MAGIC ###CSV
# MAGIC As widely used as plain text files, this common text file format captures each datum
# MAGIC or field delimited by a comma; Even though a comma is the default separator, you may use other delimiters
# MAGIC to separate fields in cases where commas are part of your data.  Popular spreadsheets
# MAGIC can generate CSV files, so it’s a popular format among data and business analysts

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading a CSV file into a DataFrame

# COMMAND ----------

# In Python
file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
df = (spark.read.format("csv")
 .option("header", "true")
 .schema(schema)
 .option("mode", "FAILFAST") # Exit if any errors
 .option("nullValue", "") # Replace any null data field with quotes
 .load(file))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading a CSV file into a Spark SQL table
# MAGIC CSV data source is no different from using Parquet or
# MAGIC JSON:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING csv
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
# MAGIC  header "true",
# MAGIC  inferSchema "true",
# MAGIC  mode "FAILFAST"
# MAGIC  )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing DataFrames to CSV files

# COMMAND ----------

# In Python
df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ####CSV data source options
# MAGIC common CSV options for DataFrameReader and Data
# MAGIC FrameWriter. Because CSV files can be complex, many options are available; for a
# MAGIC comprehensive list we refer you to the documentation.
# MAGIC - compression
# MAGIC - dateFormat
# MAGIC - multiLine
# MAGIC - inferSchema
# MAGIC  
# MAGIC  If true, Spark will determine the column data types. Default is false
# MAGIC 
# MAGIC - sep
# MAGIC 
# MAGIC Use this character to separate column values in a row. Default delimiter is a comma (,).
# MAGIC 
# MAGIC - escape
# MAGIC 
# MAGIC Use this character to escape quotes. Default is \. 
# MAGIC 
# MAGIC - header
# MAGIC 
# MAGIC Indicates whether the first line is a header
# MAGIC denoting each column name. Default is false.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Avro
# MAGIC he Avro format is used, for example, by Apache Kafka for message serializing and deserializing. It offers many benefits, including direct mapping to JSON, speed and efficiency, and bindings available for many programming languages.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading an Avro file into a DataFrame

# COMMAND ----------

# In Python
df = (spark.read.format("avro")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"))
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading an Avro file into a Spark SQL table

# COMMAND ----------

# MAGIC %sql
# MAGIC - In SQL 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW episode_tbl
# MAGIC  USING avro
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
# MAGIC  )

# COMMAND ----------

# In Python
spark.sql("SELECT * FROM episode_tbl").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing DataFrames to Avro files

# COMMAND ----------

# In Python
(df.write
 .format("avro")
 .mode("overwrite")
 .save("/tmp/data/avro/df_avro"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Avro data source options
# MAGIC common options for DataFrameReader and DataFrameWriter. 
# MAGIC 
# MAGIC - avroSchema
# MAGIC - recordName
# MAGIC - recordNamespace
# MAGIC - ignoreExtension
# MAGIC - compression

# COMMAND ----------

# MAGIC %md
# MAGIC ###ORC
# MAGIC  Spark supports a vectorized ORC reader. Two Spark configurations dictate which ORC implementation to use.
# MAGIC 
# MAGIC - A vectorized reader reads blocks of rows (often 1,024 per block) instead of one row at a time, streamlining
# MAGIC operations and reducing CPU usage for intensive operations like scans, filters, aggregations, and joins.
# MAGIC 
# MAGIC - For Hive ORC SerDe (serialization and deserialization) tables created with the SQL
# MAGIC command USING HIVE OPTIONS (fileFormat 'ORC')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading an ORC file into a DataFrame
# MAGIC To read in a DataFrame using the ORC vectorized reader, you can just use the normal
# MAGIC DataFrameReader methods and options:

# COMMAND ----------

# In Python
file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
df = spark.read.format("orc").option("path", file).load()
df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading an ORC file into a Spark SQL table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING orc
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
# MAGIC  )

# COMMAND ----------

# In Scala/Python
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing DataFrames to ORC files

# COMMAND ----------

# In Python
(df.write.format("orc")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/orc/flights_orc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Images
# MAGIC images files to support deep learning and machine learning frameworks such as TensorFlow and PyTorch.
# MAGIC For computer vision–based machine learning applications, loading and processing image data sets is important.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading an image file into a DataFrame

# COMMAND ----------

# In Python
from pyspark.ml import image
image_dir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()
images_df.select("image.height", "image.width", "image.nChannels", "image.mode",
 "label").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Binary Files
# MAGIC The DataFrameReader converts each binary file into a single DataFrame row (record) that contains the raw content and metadata of the file. The binary file data source produces a DataFrame with the following columns:
# MAGIC -  path: StringType
# MAGIC - modificationTime: TimestampType
# MAGIC - length: LongType
# MAGIC - content: BinaryType

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading a binary file into a DataFrame

# COMMAND ----------

# In Python
path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
binary_files_df = (spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .load(path))
binary_files_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC To ignore partitioning data discovery in a directory, you can set recursiveFile
# MAGIC Lookup to "true":

# COMMAND ----------

# In Python
binary_files_df = (spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .option("recursiveFileLookup", "true")
 .load(path))
binary_files_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the label column is absent when the recursiveFileLookup option is set to
# MAGIC "true".
# MAGIC 
# MAGIC Also, currently the binary file data source does not support writing a DataFrame back to
# MAGIC the original file format.