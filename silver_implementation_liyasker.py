# Databricks notebook source
# MAGIC %md
# MAGIC ### For Copying the file from raw to squad_4/bronze refer the ADF Pipeline namely "squad4_copy_bronze_liyasker" and for initial validation check the ADF pipeline namely "squad4_file_validation_liyasker"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Spark Session 

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("cust_order").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up the configuration for access the container if i used sas token for interacting with blob

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
# MAGIC ###Exploratory Data Analysis/ Profile Report for the Source Dataset

# COMMAND ----------

from ydata_profiling import ProfileReport
cust_df_pandas = cust_df.toPandas()
profile_report = ProfileReport(cust_df_pandas)

profile_report.to_notebook_iframe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Based on the above profile report Removing the records that does have phone 

# COMMAND ----------

from pyspark.sql.functions import col
# Remove rows where the 'phone' column is null or empty
cleaned_customer_df = cust_df.filter(
    (col("phone").isNotNull()) & (col("phone") != "")
)
display(cleaned_customer_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Based on the above profile report Replacing the null and empty cells of each column with appropriate value

# COMMAND ----------

from pyspark.sql.functions import when, col

# Replace null and empty values for email and state columns
final_cust_df = cleaned_customer_df.withColumn(
    "email", 
    when(col("email").isNull() | (col("email") == ""), "no email").otherwise(col("email"))
).withColumn(
    "state", 
    when(col("state").isNull() | (col("state") == ""), "state not mentioned").otherwise(col("state"))
)

# Show the result
display(final_cust_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Order and creating dataset 

# COMMAND ----------

adls_path_order = "dbfs:/mnt/landing_zone/order.csv"
order_df = spark.read.format("csv").option("header", "true").load(adls_path_order)
display(order_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Considering one to many relationship between customer and order, One customer can place multiple orders Joining order and customer based on filtered customer record

# COMMAND ----------

valid_orders_df = order_df.join(
    final_cust_df,                # Join with the customer DataFrame
    order_df.customer_id == final_cust_df["customer_id"],  # Join condition based on customer ID
    "inner"                     # Use an inner join to keep only matching rows
)

display(valid_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Order_line and creating dataset 

# COMMAND ----------

adls_path_order_line = "dbfs:/mnt/landing_zone/order_line.csv"
order_line_df = spark.read.format("csv").option("header", "true").load(adls_path_order_line)
display(order_line_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Considering one to many relationship between order_line and order, One order can have multiple orders multiple orders or products

# COMMAND ----------

valid_orders_df = valid_orders_df.join(
    order_line_df,               
    valid_orders_df.order_id == order_line_df["order_id"],  
    "inner"                    
)

display(valid_orders_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Implementing Slowly changing dimension Type 2(SCD TYPE2) for customer

# COMMAND ----------



adls_path_scd2 = "dbfs:/mnt/landing_zone/customer_SCD2_data.csv"
cust_df_scd2 = spark.read.format("csv").option("header", "true").load(adls_path_scd2)
display(cust_df_scd2)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col, trim

# Define regex to extract numeric values for Address Line 1 and non-numeric values for Address Line 2
# Address Line 1: Extract numeric values from the start of the string
address_line_1_expr = regexp_extract(col("address"), r"^\d+", 0)

# Address Line 2: Extract the remaining part of the string after the numeric part
address_line_2_expr = regexp_extract(col("address"), r"^\d+\s*(.*)", 1)

# Add two new columns for Address Line 1 and Address Line 2
customer2_df = cust_df_scd2.withColumn("address_line_1", trim(address_line_1_expr)) \
                           .withColumn("address_line_2", trim(address_line_2_expr))

# Show the updated DataFrame with split addresses
customer2_df.select("customer_id", "first_name", "last_name","address", "address_line_1", "address_line_2").show(truncate=False)



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample SCD2 customer table structure (initial loading)
customer_scd2_schema = [
    "customer_id", "first_name", "last_name", "email", "phone", "Address", 
    "address_line_2", "start_date", "end_date", "is_active"
]

# Assuming the current SCD2 table is loaded into a DataFrame called customer_scd2_df
# customer_scd2_df = spark.read.csv(existing_scd2_table_path)

# Set 'end_date' to null for active records and a past date for inactive records
scd2_active_condition = F.col("is_active") == 1
window_spec = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc())

# Set current date for new records
current_date = F.current_date()

# Add 'start_date' for the new data (changes in customer2_df)
customer2_df = customer2_df.withColumn("start_date", current_date) \
                           .withColumn("is_active", F.lit(1)) \
                           .withColumn("end_date", F.lit(None).cast("date"))

# Step 1: Deactivate records in the current SCD2 table that are now changed
# Join on customer_id and look for changes in the address or phone details
changed_customers_df = cust_df_scd2.join(
    customer2_df, 
    cust_df_scd2["customer_id"] == customer2_df["customer_id"], 
    "inner"
).filter(
    (customer2_df["Address"] != customer2_df["Address"]) |
    (customer2_df["address_line_2"] != customer2_df["address_line_2"]) |
    (customer2_df["phone"] != customer2_df["phone"])
).select(cust_df_scd2["customer_id"])

# Step 2: Update the old records with the 'end_date' and set 'is_active' to 0
updated_scd2_df = cust_df_scd2.join(
    changed_customers_df, 
    on="customer_id", 
    how="left_semi"
).withColumn(
    "end_date", current_date
).withColumn(
    "is_active", F.lit(0)
)

# Step 3: Insert the new records for the customers that have changed (with updated address)
# The new records should have start_date as the current date and is_active as 1
new_customer_scd2_df = customer2_df.join(
    updated_scd2_df, 
    on="customer_id", 
    how="left_anti"
)

# Combine the updated SCD2 table and the new records
final_scd2_df = updated_scd2_df.unionByName(new_customer_scd2_df)

display(final_scd2_df)



# COMMAND ----------

# Write final SCD2 data back to Delta Table (or a new location)
scd2_delta_path = "abfss://bayerstorage@bayershackadls.dfs.core.windows.net/delta/customer_scd2"
final_scd2_df.write.format("delta").mode("overwrite").save(scd2_delta_path)
