# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

## load new data
load_new_json_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/books-cdc/02.json`;

# COMMAND ----------


