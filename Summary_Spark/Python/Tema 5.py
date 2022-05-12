# Databricks notebook source
# DBTITLE 1,Tema 5
# MAGIC %md
# MAGIC #Spark SQL and DataFrames: Interacting with External Data Sources

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark SQL and Apache Hive
# MAGIC Spark SQL is a foundational component of Apache Spark that integrates relational
# MAGIC processing with Spark’s functional programming API. Its genesis was in previous
# MAGIC work on Shark. Shark was originally built on the Hive codebase on top of Apache Spark1
# MAGIC and became one of the first interactive SQL query engines on Hadoop systems.
# MAGIC 
# MAGIC It demonstrated that it was possible to have the best of both worlds; as fast as an
# MAGIC enterprise data warehouse, and scaling as well as Hive/MapReduce.

# COMMAND ----------

# MAGIC %md
# MAGIC ###User-Defined Functions
# MAGIC the flexibility of Spark
# MAGIC allows for data engineers and data scientists to define their own functions too. These
# MAGIC are known as user-defined functions (UDFs).

# COMMAND ----------

# MAGIC %md
# MAGIC ####Spark SQL UDFs
# MAGIC The benefit of creating your own PySpark or Scala UDFs is that you (and others) will
# MAGIC be able to make use of them within Spark SQL itself. For example, a data scientist can
# MAGIC wrap an ML model within a UDF so that a data analyst can query its predictions in
# MAGIC Spark SQL without necessarily understanding the internals of the model.
# MAGIC 
# MAGIC Here’s a simplified example of creating a Spark SQL UDF. Note that UDFs operate per
# MAGIC session and they will not be persisted in the underlying metastore:

# COMMAND ----------

# In Python
from pyspark.sql.types import LongType
# Create cubed function
def cubed(s):
 return s * s * s
# Register UDF
spark.udf.register("cubed", cubed, LongType())
# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

# MAGIC %md
# MAGIC You can now use Spark SQL to execute either of these cubed() functions:

# COMMAND ----------

# MAGIC %scala
# MAGIC // In Scala/Python
# MAGIC // Query the cubed UDF
# MAGIC spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Evaluation order and null checking in Spark SQL
# MAGIC Spark SQL does not guarantee the order of evaluation of subexpressions. For example, the following query
# MAGIC does not guarantee that the s is NOT NULL clause is executed prior to the strlen(s)

# COMMAND ----------

spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1")

# COMMAND ----------

# MAGIC %md
# MAGIC Therefore, to perform proper null checking, it is recommended that you do the
# MAGIC following:
# MAGIC 1. Make the UDF itself null-aware and do null checking inside the UDF.
# MAGIC 2. Use IF or CASE WHEN expressions to do the null check and invoke the UDF in a
# MAGIC conditional branch.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Speeding up and distributing PySpark UDFs with Pandas UDFs
# MAGIC One of the previous prevailing issues with using PySpark UDFs was that they had
# MAGIC slower performance than Scala UDFs. This was because the PySpark UDFs required
# MAGIC data movement between the JVM and Python, which was quite expensive. To resolve
# MAGIC this problem, Pandas UDFs (also known as vectorized UDFs) were introduced as part
# MAGIC of Apache
# MAGIC 
# MAGIC You define a Pandas UDF using the keyword pandas_udf as
# MAGIC the decorator, or to wrap the function itself. Once the data is in Apache Arrow format, there is no longer the need to serialize/pickle the data as it is already in a format consumable by the Python process. 
# MAGIC 
# MAGIC  Pandas UDFs were split into two API categories: Pandas UDFs and Pandas Function APIs.
# MAGIC  - Pandas UDFs
# MAGIC  
# MAGIC  - Pandas Function APIs
# MAGIC  
# MAGIC  The following is an example of a scalar Pandas UDF

# COMMAND ----------

# In Python
# Import pandas
import pandas as pd
# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
 return a * a * a
# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())

#Let’s start with a simple Pandas Series (as defined for x) and then apply the local function cubed() for the cubed calculation:
# Create a Pandas Series
x = pd.Series([1, 2, 3])
# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

#Now let’s switch to a Spark DataFrame. We can execute this function as a Spark vectorized UDF as follows:
# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 4)
# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Querying with the Spark SQL Shell, Beeline, and Tableau

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using the Spark SQL Shell
# MAGIC A convenient tool for executing Spark SQL queries is the spark-sql CLI. While this utility communicates with the Hive metastore service in local mode, it does not talk to the Thrift JDBC/ODBC server.
# MAGIC 
# MAGIC To start the Spark SQL CLI, execute the following command in the $SPARK_HOME folder: 
# MAGIC 
# MAGIC *./bin/spark-sql*
# MAGIC 
# MAGIC - Create a table
# MAGIC 
# MAGIC  To create a new permanent Spark SQL table:
# MAGIC  
# MAGIC  *CREATE TABLE people (name STRING, age int);*
# MAGIC  
# MAGIC  - Insert data into the table
# MAGIC  
# MAGIC  *INSERT INTO people SELECT name, age FROM ...*
# MAGIC  
# MAGIC  As you’re not dependent on loading data from a preexisting table or file, you can
# MAGIC insert data into the table using INSERT...VALUES statements
# MAGIC  
# MAGIC  *INSERT INTO people VALUES ("Samantha", 19);*
# MAGIC  
# MAGIC  - Running a Spark SQL query
# MAGIC  
# MAGIC  *SHOW TABLES;*
# MAGIC  
# MAGIC  *SELECT name FROM people WHERE age IS NULL;*

# COMMAND ----------

# MAGIC %md
# MAGIC ###Working with Beeline
# MAGIC Beeline is a JDBC client based on the SQLLine CLI. You can use this same utility to execute
# MAGIC Spark SQL queries against the Spark Thrift server. 
# MAGIC 
# MAGIC - Start the Thrift server
# MAGIC 
# MAGIC To start the Spark Thrift JDBC/ODBC server, execute the following command from
# MAGIC the $SPARK_HOME folder:

# COMMAND ----------

# MAGIC %md
# MAGIC ##External Data Sources

# COMMAND ----------

# MAGIC %md
# MAGIC ###JDBC and SQL Databases
# MAGIC Spark SQL includes a data source API that can read data from other databases using
# MAGIC JDBC. It simplifies querying these data sources as it returns the results as a DataFrame, thus providing all of the benefits of Spark SQL (including performance and the ability to join with other data sources).
# MAGIC 
# MAGIC you will need to specify the JDBC driver for your JDBC data source
# MAGIC and it will need to be on the Spark classpath. From the $SPARK_HOME folder, you’ll
# MAGIC issue a command like the following:
# MAGIC 
# MAGIC *./bin/spark-shell --driver-class-path $database.jar --jars $database.jar*
# MAGIC 
# MAGIC Using the data source API, the tables from the remote database can be loaded as a
# MAGIC DataFrame or Spark SQL temporary view. Users can specify the JDBC connection
# MAGIC properties in the data source options.
# MAGIC 
# MAGIC - The importance of partitioning
# MAGIC 
# MAGIC When transferring large amounts of data between Spark SQL and a JDBC external
# MAGIC source, it is important to partition your data source. All of your data is going through
# MAGIC one driver connection, which can saturate and significantly slow down the performance of your extraction, as well as potentially saturate the resources of your source
# MAGIC system.
# MAGIC 
# MAGIC A good starting point for numPartitions is to use a multiple of the number of
# MAGIC Spark workers. For example, if you have four Spark worker nodes, then perhaps
# MAGIC start with 4 or 8 partitions. But it is also important to note how well your source
# MAGIC system can handle the read requests. 
# MAGIC 
# MAGIC Initially, calculate the lowerBound and upperBound based on the minimum and
# MAGIC maximum partitionColumn actual values.

# COMMAND ----------

# MAGIC %md
# MAGIC ###PostgreSQL
# MAGIC The following examples show how to load from and save to a PostgreSQL database
# MAGIC using the Spark SQL data source API and JDBC:

# COMMAND ----------

# In Python
# Read Option 1: Loading data from a JDBC source using load method
jdbcDF1 = (spark
           .read
           .format("jdbc")
           .option("url", "jdbc:postgresql://[DBSERVER]")
           .option("dbtable", "[SCHEMA].[TABLENAME]")
           .option("user", "[USERNAME]")
           .option("password", "[PASSWORD]")
           .load())
# Read Option 2: Loading data from a JDBC source using jdbc method
jdbcDF2 = (spark
 .read
 .jdbc("jdbc:postgresql://[DBSERVER]", "[SCHEMA].[TABLENAME]",
 properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))
# Write Option 1: Saving data to a JDBC source using save method
(jdbcDF1
 .write
 .format("jdbc")
 .option("url", "jdbc:postgresql://[DBSERVER]")
 .option("dbtable", "[SCHEMA].[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())
# Write Option 2: Saving data to a JDBC source using jdbc method
(jdbcDF2
 .write
 .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]",
 properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))


# COMMAND ----------

# MAGIC %md
# MAGIC ###MySQL
# MAGIC The following examples show how to load data from and save it to a MySQL database
# MAGIC using the Spark SQL data source API and JDBC

# COMMAND ----------

# In Python
# Loading data from a JDBC source using load 
jdbcDF = (spark
 .read
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .load())
# Saving data to a JDBC source using save 
(jdbcDF
 .write
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Azure Cosmos DB
# MAGIC The following examples show how to load data from and save it to an Azure Cosmos
# MAGIC DB database using the Spark SQL data source API and JDBC in Scala and PySpark.

# COMMAND ----------

# In Python
# Loading data from Azure Cosmos DB
# Read configuration
query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
readConfig = {
 "Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
 "Masterkey" : "[MASTER KEY]",
 "Database" : "[DATABASE]",
 "preferredRegions" : "Central US;East US2",
"Collection" : "[COLLECTION]",
 "SamplingRatio" : "1.0",
 "schema_samplesize" : "1000",
 "query_pagesize" : "2147483647",
 "query_custom" : query
}
# Connect via azure-cosmosdb-spark to create Spark DataFrame
df = (spark
 .read
 .format("com.microsoft.azure.cosmosdb.spark")
 .options(**readConfig)
 .load())
# Count the number of flights
df.count()
# Saving data to Azure Cosmos DB
# Write configuration
writeConfig = {
"Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
"Masterkey" : "[MASTER KEY]",
"Database" : "[DATABASE]",
"Collection" : "[COLLECTION]",
"Upsert" : "true"
}
# Upsert the DataFrame to Azure Cosmos DB
(df.write
 .format("com.microsoft.azure.cosmosdb.spark")
 .options(**writeConfig)
 .save())

# COMMAND ----------

# MAGIC %md
# MAGIC ###MS SQL Server
# MAGIC The following examples show how to load data from and save it to an MS SQL Server
# MAGIC database using the Spark SQL data source API and JDBC in Scala and PySpark:

# COMMAND ----------

# In Python
# Configure jdbcUrl
jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
# Loading data from a JDBC source
jdbcDF = (spark
 .read
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .load())
# Saving data to a JDBC source
(jdbcDF
 .write
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Higher-Order Functions in DataFrames and Spark SQL
# MAGIC Because complex data types are amalgamations of simple data types, it is tempting to
# MAGIC manipulate them directly. There are two typical solutions for manipulating complex
# MAGIC data types:
# MAGIC - Exploding the nested structure into individual rows, applying some function, and
# MAGIC then re-creating the nested structure
# MAGIC - Building a user-defined function
# MAGIC 
# MAGIC These approaches have the benefit of allowing you to think of the problem in tabular
# MAGIC format. They typically involve (but are not limited to) using utility functions such as get_json_object(), from_json(), to_json(), explode(), and selectExpr()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Option 1: Explode and Collect
# MAGIC In this nested SQL statement, we first explode(values), which creates a new row
# MAGIC (with the id) for each element (value) within values:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC SELECT id, collect_list(value + 1) AS values
# MAGIC FROM (SELECT id, EXPLODE(values) AS value
# MAGIC  FROM table) x
# MAGIC GROUP BY id

# COMMAND ----------

# MAGIC %md
# MAGIC While collect_list() returns a list of objects with duplicates, the GROUP BY statement requires shuffle operations, meaning the order of the re-collected array isn’t necessarily the same as that of the original array. As values could be any number of
# MAGIC dimensions (a really wide and/or really long array) and we’re doing a GROUP BY, this
# MAGIC approach could be very expensive.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Option 2: User-Defined Function
# MAGIC To perform the same task (adding 1 to each element in values), we can also create a
# MAGIC UDF that uses map() to iterate through each element (value) and perform the addition operation
# MAGIC 
# MAGIC We could then use this UDF in Spark SQL as follows:

# COMMAND ----------

# MAGIC %sql
# MAGIC spark.sql("SELECT id, plusOneInt(values) AS values FROM table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC While this is better than using explode() and collect_list() as there won’t be any
# MAGIC ordering issues, the serialization and deserialization process itself may be expensive.
# MAGIC It’s also important to note, however, that collect_list() may cause executors to
# MAGIC experience out-of-memory issues for large data sets, whereas using UDFs would alle‐
# MAGIC viate these issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Built-in Functions for Complex Data Types
# MAGIC Instead of using these potentially expensive techniques, you may be able to use some of the built-in functions for complex data types included as part of Apache Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ###Higher-Order Functions
# MAGIC In addition to the previously noted built-in functions, there are higher-order func‐
# MAGIC tions that take anonymous lambda functions as arguments. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC transform(values, value -> lambda expression)

# COMMAND ----------

# MAGIC %md
# MAGIC The transform() function takes an array (values) and anonymous function (lambda
# MAGIC expression) as input. The function transparently creates a new array by applying the
# MAGIC anonymous function to each element, and then assigning the result to the output
# MAGIC array (similar to the UDF approach, but more efficiently).
# MAGIC 
# MAGIC Let’s create a sample data set so we can run some examples:

# COMMAND ----------

# In Python
from pyspark.sql.types import *
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")
# Show the DataFrame
t_c.show()

# COMMAND ----------

# MAGIC %md
# MAGIC With the preceding DataFrame you can run the following higher-order function
# MAGIC queries.
# MAGIC 
# MAGIC - transform()
# MAGIC *transform(array<T>, function<T, U>): array<U>*
# MAGIC   
# MAGIC The transform() function produces an array by applying a function to each element
# MAGIC of the input array (similar to a map() function):

# COMMAND ----------

# In Scala/Python
# Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""SELECT celsius,transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC - filter()
# MAGIC *filter(array<T>, function<T, Boolean>): array<T>*
# MAGIC   
# MAGIC The filter() function produces an array consisting of only the elements of the input
# MAGIC array for which the Boolean function is true:

# COMMAND ----------

# In Scala/Python
# Filter temperatures > 38C for array of temperatures
spark.sql("""
SELECT celsius, 
 filter(celsius, t -> t > 38) as high 
 FROM tC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC - exists()
# MAGIC *exists(array<T>, function<T, V, Boolean>): Boolean*
# MAGIC   
# MAGIC The exists() function returns true if the Boolean function holds for any element in
# MAGIC the input array:

# COMMAND ----------

# In Scala/Python
# Is there a temperature of 38C in the array of temperatures
spark.sql("""
SELECT celsius, 
 exists(celsius, t -> t = 38) as threshold
 FROM tC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC - reduce()
# MAGIC *reduce(array<T>, B, function<B, T, B>, function<B, R>)*
# MAGIC   
# MAGIC The reduce() function reduces the elements of the array to a single value by merging
# MAGIC the elements into a buffer B using function<B, T, B> and applying a finishing
# MAGIC function<B, R> on the final buffer:

# COMMAND ----------

# In Scala/Python
# Calculate average temperature and convert to F
spark.sql("""
SELECT celsius, 
 reduce(
 celsius, 
 0, 
 (t, acc) -> t + acc, 
 acc -> (acc div size(celsius) * 9 div 5) + 32
 ) as avgFahrenheit 
 FROM tC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Common DataFrames and Spark SQL Operations
# MAGIC Part of the power of Spark SQL comes from the wide range of DataFrame operations
# MAGIC (also known as untyped Dataset operations) it supports. The list of operations is quite
# MAGIC extensive and includes:
# MAGIC - Aggregate functions
# MAGIC - Collection functions
# MAGIC - Datetime functions
# MAGIC - Math functions
# MAGIC - Miscellaneous functions
# MAGIC - Non-aggregate functions
# MAGIC - Sorting functions
# MAGIC - String functions
# MAGIC - UDF functions
# MAGIC - Window functions
# MAGIC 
# MAGIC we will focus on the following common relational operations:
# MAGIC - Unions and joins
# MAGIC - Windowing
# MAGIC - Modifications
# MAGIC 
# MAGIC To perform these DataFrame operations, we’ll first prepare some data. In the following code snippet, we:
# MAGIC 1. Import two files and create two DataFrames, one for airport (airportsna) information and one for US flight delays (departureDelays).
# MAGIC 2. Using expr(), convert the delay and distance columns from STRING to INT.
# MAGIC 3. Create a smaller table, foo, that we can focus on for our demo examples; it contains only information on three flights originating from Seattle (SEA) to the destination of San Francisco (SFO) for a small time range.

# COMMAND ----------

# In Python
# Set file paths
from pyspark.sql.functions import expr
tripdelaysFilePath =
 "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath =
 "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
 
# Obtain airports data set
airportsna = (spark.read
 .format("csv")
 .options(header="true", inferSchema="true", sep="\t")
 .load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")
# Obtain departure delays data set
departureDelays = (spark.read
 .format("csv")
 .options(header="true")
 .load(tripdelaysFilePath))
departureDelays = (departureDelays
 .withColumn("delay", expr("CAST(delay as INT) as delay"))
 .withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")
# Create temporary small table
foo = (departureDelays
 .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
 date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

# COMMAND ----------

# MAGIC %md
# MAGIC The departureDelays DataFrame contains data on >1.3M flights while the foo DataFrame contains just three rows with information on flights from SEA to SFO for a specific time range, as noted in the following output:

# COMMAND ----------

# Scala/Python
spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

# COMMAND ----------

spark.sql("SELECT * FROM foo").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unions
# MAGIC A common pattern within Apache Spark is to union two different DataFrames with
# MAGIC the same schema together. This can be achieved using the union() method:

# COMMAND ----------

# In Python
# Union two tables
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")
# Show the union (filtering for SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The bar DataFrame is the union of foo with delays. Using the same filtering criteria
# MAGIC results in the bar DataFrame, we see a duplication of the foo data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC spark.sql("""
# MAGIC SELECT * 
# MAGIC  FROM bar 
# MAGIC  WHERE origin = 'SEA' 
# MAGIC  AND destination = 'SFO' 
# MAGIC  AND date LIKE '01010%' 
# MAGIC  AND delay > 0
# MAGIC """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joins
# MAGIC A common DataFrame operation is to join two DataFrames (or tables) together. By
# MAGIC default, a Spark SQL join is an inner join, with the options being inner, cross,
# MAGIC outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and
# MAGIC left_anti. 
# MAGIC 
# MAGIC The following code sample performs the default of an inner join between the air
# MAGIC portsna and foo DataFrames:

# COMMAND ----------

# In Python
# Join departure delays data (foo) with airport info
foo.join(
 airports,
 airports.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC spark.sql("""
# MAGIC SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
# MAGIC  FROM foo f
# MAGIC  JOIN airports_na a
# MAGIC  ON a.IATA = f.origin
# MAGIC """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The preceding code allows you to view the date, delay, distance, and destination
# MAGIC information from the foo DataFrame joined to the city and state information from
# MAGIC the airports DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ###Windowing
# MAGIC A window function uses values from the rows in a window (a range of input rows) to
# MAGIC return a set of values, typically in the form of another row. With window functions, it
# MAGIC is possible to operate on a group of rows while still returning a single value for every
# MAGIC input row. 
# MAGIC 
# MAGIC Let’s start with a review of the TotalDelays (calculated by sum(Delay)) experienced
# MAGIC by flights originating from Seattle (SEA), San Francisco (SFO), and New York City
# MAGIC (JFK) and going to a specific set of destination locations, as noted in the following
# MAGIC query:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC DROP TABLE IF EXISTS departureDelaysWindow;
# MAGIC 
# MAGIC CREATE TABLE departureDelaysWindow AS
# MAGIC SELECT origin, destination, SUM(delay) AS TotalDelays
# MAGIC  FROM departureDelays
# MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK')
# MAGIC  AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
# MAGIC GROUP BY origin, destination;
# MAGIC 
# MAGIC SELECT * FROM departureDelaysWindow

# COMMAND ----------

# MAGIC %md
# MAGIC You could achieve this by running three different queries for each origin and then unioning the results together, like this:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
# MAGIC FROM departureDelaysWindow
# MAGIC WHERE origin = '[ORIGIN]'
# MAGIC GROUP BY origin, destination
# MAGIC ORDER BY SUM(TotalDelays) DESC
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC where [ORIGIN] is the three different origin values of JFK, SEA, and SFO.
# MAGIC 
# MAGIC But a better approach would be to use a window function like dense_rank() to perform the following calculation:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC spark.sql("""
# MAGIC SELECT origin, destination, TotalDelays, rank 
# MAGIC  FROM ( 
# MAGIC  SELECT origin, destination, TotalDelays, dense_rank() 
# MAGIC  OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
# MAGIC  FROM departureDelaysWindow
# MAGIC  ) t 
# MAGIC  WHERE rank <= 3
# MAGIC """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC By using the dense_rank() window function, we can quickly ascertain that the destinations with the worst delays for the three origin cities
# MAGIC 
# MAGIC It’s important to note that each window grouping needs to fit in a single executor and will get composed into a single partition during execution. Therefore, you need to ensure that your queries are not unbounded (i.e., limit the size of your window).

# COMMAND ----------

# MAGIC %md
# MAGIC ###Modifications
# MAGIC Another common operation is to perform modifications to the DataFrame. While
# MAGIC DataFrames themselves are immutable, you can modify them through operations that
# MAGIC create new, different DataFrames, with different columns
# MAGIC 
# MAGIC  Let’s start with our
# MAGIC previous small DataFrame example:

# COMMAND ----------

# In Scala/Python
foo.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new columns

# COMMAND ----------

# In Python
from pyspark.sql.functions import expr
foo2 = (foo.withColumn(
 "status",
 expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
 ))
foo2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dropping columns

# COMMAND ----------

# In Python
foo3 = foo2.drop("delay")
foo3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming columns

# COMMAND ----------

# In Python
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pivoting
# MAGIC sometimes you will need to swap the columns for the rows, for example:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
# MAGIC  FROM departureDelays
# MAGIC WHERE origin = 'SEA'

# COMMAND ----------

# MAGIC %md
# MAGIC Pivoting allows you to place names in the month column (instead of 1 and 2 you can
# MAGIC show Jan and Feb, respectively) as well as perform aggregate calculations (in this case
# MAGIC average and max) on the delays by destination and month:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC SELECT * FROM (
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
# MAGIC  FROM departureDelays WHERE origin = 'SEA'
# MAGIC )
# MAGIC PIVOT (
# MAGIC  CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
# MAGIC  FOR month IN (1 JAN, 2 FEB)
# MAGIC )
# MAGIC ORDER BY destination