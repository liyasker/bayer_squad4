# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Spark Session 

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("cust_order").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up the configuration for access the container

# COMMAND ----------

#For SAS Config

sas_token = "" 
spark.conf.set("fs.azure.account.auth.type.bayerstorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.bayerstorage.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.sas.SASTokenProvider")
spark.conf.set("fs.azure.sas.token.bayerstorage.dfs.core.windows.net", sas_token)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the files from ADLS

# COMMAND ----------


from pyspark.sql import SparkSession

# Check if the directory exists
try:
    files = dbutils.fs.ls("mnt/landing_zone")
    display(files)
except Exception as e:
    print(f"Error: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the csv file which is mounted

# COMMAND ----------

adls_path = "dbfs:/mnt/landing_zone/customer.csv"
cust_df = spark.read.format("csv").option("header", "true").load(adls_path)
display(cust_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing ydata-profling to generate Profile report/ Exploratory Data Analysis of the source dataset 

# COMMAND ----------

# MAGIC %pip install ydata-profiling

# COMMAND ----------

# MAGIC %md
# MAGIC ###Exploratory Data Analysis for the Source Dataset

# COMMAND ----------

from ydata_profiling import ProfileReport
cust_df_pandas = cust_df.toPandas()
profile_report = ProfileReport(cust_df_pandas)

profile_report.to_notebook_iframe()

# COMMAND ----------

from pyspark.sql.functions import col
# Remove rows where the 'phone' column is null or empty
cleaned_customer_df = cust_df.filter(
    (col("phone").isNotNull()) & (col("phone") != "")
)
display(cleaned_customer_df)


# COMMAND ----------


