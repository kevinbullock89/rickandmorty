# Databricks notebook source
# MAGIC %run /Workspace/Repos/kevin.bullock@macaw.nl/rickandmorty/conf/constants

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import explode, col, abs, hash

# COMMAND ----------

def load_rick_and_morty_data_to_bronze(table_name, url):

    counter = 0
    while True:

        path = f"{MOUNT_POINT}/{bronze}/{table_name}/{table_name}_{counter}.json"

        # Send a GET request to the specified URL and store the response in the variable "response"
        response = requests.get(url)

        # Extract the JSON data from the response and store it in the variable "response_dict"
        response_dict = response.json()

        response_dumps = json.dumps(response_dict)

        url = response_dict['info']['next']

        if dbutils.fs.rm(path, True):
            print("Deleted existing file: ", path)

        # Write the response_dumps to the specified file path using dbutils.fs.put()
        dbutils.fs.put(path, response_dumps)

        if url:
            counter += 1
        else:
            break

# COMMAND ----------

def load_rick_and_morty_bronze_to_silver(bronze_path_characters, bronze_table):
    df = spark.read.option("multiline", "true").json(bronze_path_characters)

    flatterned_df = df.select(explode("results"))

    if bronze_table == "Characters":

        select_flattened_df = flatterned_df.select(
            abs(hash(col("col.id"))).alias("id"),
            col("col.name"),
            col("col.species"),
            col("col.origin.name").alias("origin_name"),
            col("col.status"),
            col("col.location.name").alias("location_name"),
        )

        print(f"load data {select_flattened_df}")
        select_flattened_df.write.format("delta").option(
            "path", f"{MOUNT_POINT}/{silver}/{characters_table}"
        ).mode("overwrite").saveAsTable(f"{silver}.{characters_table}")

    elif bronze_table == "Location":
        df_final = flatterned_df.select(
            abs(hash(col("col.id"), col("col.name"))).alias("id"),
            col("col.name"),
            col("col.type"),
            col("col.dimension"),
        )

        print(f"load data {df_final}")
        df_final.write.format("delta").option(
            "path", f"{MOUNT_POINT}/{silver}/{location_table}"
        ).mode("overwrite").saveAsTable(f"{silver}.{location_table}")

# COMMAND ----------


