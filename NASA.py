# Databricks notebook source

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import re
import pandas as pd
import glob

ruta = "/FileStore/tables/access_log_Jul95"
base_df = spark.read.text(ruta)

base_df.printSchema()
#base_df.show()

#hostnames 199.72.81.55 /  unicomp6.unicomp.net
host = '(\S+)'
           
#timestamps 01/Jul/1995:00:00:01 -0400
#ts = '\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
#timestamps 01/Jul/1995:00:00:01
ts = '(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})'

#HTTP ‘GET’, ‘/history/apollo/’, ‘HTTP/1.0’
method_uri_protocol = '\"(\S+)\s(\S+)\s*(\S*)\"'

#HTTP status codes 200
status = '\s(\d{3})\s'

#HTTP response content size 6245
content_size = '\s(\d+)$'

#Putting it all together
logs_df = base_df.select(regexp_extract('value', host, 1).alias('host'),
                         regexp_extract('value', ts, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol, 2).alias('resource'),
                         regexp_extract('value', method_uri_protocol, 3).alias('protocol'),
                         regexp_extract('value', status, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size, 1).cast('integer').alias('content_size'))

logs_df2 = logs_df.withColumn("timestamp",to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss"))

logs_df2.show(10, truncate=False)
print((logs_df.count(), len(logs_df.columns)))

(logs_df2.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/FileStore/tables/access_log_Jul95_update2.parquet"))

# COMMAND ----------

file = "/FileStore/tables/access_log_Jul95_update2.parquet"
df = spark.read.format("parquet").load(file)

# COMMAND ----------

# DBTITLE 1,¿Cuáles son los distintos protocolos web utilizados?
df.select('protocol').distinct().show()

# COMMAND ----------

# DBTITLE 1,¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos  para ver cuál es el más común.
df.groupBy('status').count().orderBy('count', ascending=False).show()

# COMMAND ----------

# DBTITLE 1,¿Y los métodos de petición (verbos) más utilizados?
df.select('method') \
.groupby('method') \
.count() \
.orderBy('count', ascending=False) \
.show()

# COMMAND ----------

# DBTITLE 1,¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
df.select(F.max('content_size').alias('maximo_bytes')).show()

# COMMAND ----------

# DBTITLE 1,Que recurso de nuestra web es el que más tráfico recibe. Es  decir, el recurso con más registros
df.select('resource').groupBy('resource').count().orderBy('count', ascending=False).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,¿Qué días la web recibió más tráfico?
df.select(to_date(col("timestamp"),"dd/MM/yyyy").alias("date")).groupBy('date').count().orderBy('count', ascending=False).show()

# COMMAND ----------

# DBTITLE 1,¿Cuáles son los hosts son los más frecuentes?
df.groupBy('host').count().orderBy('count', ascending=False).show(5,truncate=False)

# COMMAND ----------

# DBTITLE 1, ¿A qué horas se produce el mayor número de tráfico en la web?
#por horas de dias
df.select('status','timestamp').groupby('timestamp').count().orderBy('count', ascending=False).show(truncate=False)
#por horas
df.select(date_format('timestamp', 'HH:mm:ss').alias('date')).groupby('date').count().orderBy('count', ascending=False).show(truncate=False)

# COMMAND ----------

# DBTITLE 1, ¿Cuál es el número de errores 404 que ha habido cada día?
df.select(to_date(col("timestamp"),"dd/MM/yyyy").alias("date")).where(col('status') == '404').groupBy('date').count().orderBy('count', ascending=False).show()
