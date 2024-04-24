# Databricks notebook source
# MAGIC %run /Workspace/Repos/kevin.bullock@macaw.nl/rickandmorty/conf/functions

# COMMAND ----------

load_rick_and_morty_bronze_to_silver(bronze_path_characters, characters_table)

# COMMAND ----------

load_rick_and_morty_bronze_to_silver(bronze_path_location, location_table)
