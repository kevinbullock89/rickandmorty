# Databricks notebook source
# MAGIC %run /Workspace/Repos/kevin.bullock@macaw.nl/rickandmorty/conf/functions

# COMMAND ----------

load_rick_and_morty_data_to_bronze(characters_table,characters_url)

# COMMAND ----------

load_rick_and_morty_bronze_to_silver(bronze_path_characters)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY `20_silver`.characters

# COMMAND ----------


