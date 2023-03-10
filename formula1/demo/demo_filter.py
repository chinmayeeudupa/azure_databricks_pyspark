# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"/mnt/myformula1dlake/processed/races")

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019")

# COMMAND ----------

races_filtered_df = races_df.filter(races_df["race_year"] == 2019)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year == 2019 and round <= 5")

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


