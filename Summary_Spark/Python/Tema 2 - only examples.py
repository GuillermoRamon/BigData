# Databricks notebook source
# MAGIC %md
# MAGIC #Tema 2 del libro - Getting Started

# COMMAND ----------

# DBTITLE 1,Transformations, Actions, and Lazy Evaluation
# In Python 
#strings = spark.read.text("C:/spark/spark-3.0.3-bin-hadoop2.7/README.md")
strings = spark.read.text("/FileStore/tables/README.md")

filtered = strings.filter(strings.value.contains("Spark"))
filtered.count()

# COMMAND ----------

# DBTITLE 1,Counting M&Ms for the Cookie Monster
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

    count_mnm_df.show(n=60,truncate=False)

    print("Total Rows = %d" % (count_mnm_df.count()))

    ca_count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))

    ca_count_mnm_df.show(n=10, truncate=False)

    #spark.stop()