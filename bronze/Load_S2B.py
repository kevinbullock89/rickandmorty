# Databricks notebook source
# MAGIC %run /Workspace/Repos/kevin.bullock@macaw.nl/rickandmorty/conf/functions

# COMMAND ----------

load_rick_and_morty_data_to_bronze(characters_table,characters_url)

# COMMAND ----------

load_rick_and_morty_data_to_bronze(location_table, locations_url)

# COMMAND ----------


