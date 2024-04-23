# Databricks notebook source
MOUNT_POINT = "/mnt/dls"
bronze = '10_bronze'
silver = '20_silver'
characters_table = 'Characters'
location_table = 'Location'
Episode_table = 'Episode'

spark.conf.set("20.silver",silver)

base_url = 'https://rickandmortyapi.com/api'

characters_url = 'https://rickandmortyapi.com/api/character'
locations_url = 'https://rickandmortyapi.com/api/location'
episodes_url = 'https://rickandmortyapi.com/api/episode'

bronze_path_characters = f"{MOUNT_POINT}/{bronze}/{characters_table}/*.json"
