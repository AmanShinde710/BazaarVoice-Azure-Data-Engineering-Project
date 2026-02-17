# Databricks notebook source
# MAGIC %md
# MAGIC # **Bronze to Silver**

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter

# COMMAND ----------

file_name = ''

# COMMAND ----------

# DBTITLE 1,Untitled
capitalized_file_name = file_name.capitalize()

#catalog
catalog_name = 'bv_cata'

#Bronze
bronze_path = 'abfss://bronze@bvprojectstg.dfs.core.windows.net/'
bronze_file_path = bronze_path + file_name + '/'

#Silver
silver_db_name = 'silver'
silver_table_name = file_name
silver_db_path = 'abfss://silver@bvprojectstg.dfs.core.windows.net/'
silver_table_path = silver_db_path + capitalized_file_name + '/'


# COMMAND ----------

catalog_query = f'''
                CREATE CATALOG IF NOT EXISTS {catalog_name}
                MANAGED LOCATION '{silver_db_path}'
'''
spark.sql(catalog_query)

# COMMAND ----------

query = f'''
          CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_db_name}         
      '''
spark.sql(query)

# COMMAND ----------

df = spark.read.format('csv')\
        .option('header',True)\
        .option('inferSchema',True)\
        .load(bronze_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding a column etl_date which contains the date of last_ingestion

# COMMAND ----------

df = df.withColumn('etl_date', lit(current_date()))

# COMMAND ----------

if file_name == 'reviews':
    df.write.format('delta')\
        .mode('append')\
        .option('mergeSchema',True)\
        .save(silver_table_path)

elif file_name == 'categories':
    df.write.format('delta')\
        .mode('overwrite')\
        .option('mergeSchema',True)\
        .option('overwriteMode',True)\
        .save(silver_table_path)

elif file_name == 'products':
    df.createOrReplaceTempView('product')

    e_df = spark.createDataFrame([] , df.schema)

    df.write.format('delta')\
        .mode('append')\
        .option('mergeSchema',True)\
        .save(silver_table_path)

    # Load the target delta table
    products_table = DeltaTable.forPath(spark, silver_table_path)
    products_table.alias("T") \
      .merge(
        df.alias("S"),
        "T.ProductID = S.ProductID") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()