# Databricks notebook source
# MAGIC %md
# MAGIC ### 3.2 - Auto Loader

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
    .load(f"{dataset_bookstore}/orders-raw")
.writeStream
    .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
    .table("orders_updates")
)

# COMMAND ----------

chk_pnt = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/")
display(chk_pnt)

# COMMAND ----------

meta_data = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/metadata")
display(meta_data)

# COMMAND ----------

off_sets = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/offsets")
display(off_sets)

# COMMAND ----------

sources = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/sources/0/rocksdb/logs")
display(sources)

# COMMAND ----------

commits = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/commits")
display(commits)

# COMMAND ----------

schemas = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/_schemas/")
display(schemas)

# COMMAND ----------

tmp_path_dir = dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint/__tmp_path_dir/")
display(tmp_path_dir)

# COMMAND ----------

schms = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/_schemas/0")
display(schms)

# COMMAND ----------

ofsts = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/offsets/1")
display(ofsts)

# COMMAND ----------

cmts = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/commits/1")
display(cmts)

# COMMAND ----------

srcs = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/sources/0/metadata")
display(srcs)

src0 = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/sources/0/rocksdb/0.zip")
display(src0)

src1 = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/sources/0/rocksdb/1.zip")
display(src1)

# COMMAND ----------


src_logs = dbutils.fs.head("dbfs:/mnt/demo/orders_checkpoint/sources/0/rocksdb/logs/000005-67972b36-17c9-4c14-a856-1ff16c026713.log")
display(src_logs)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates;

# COMMAND ----------

# loading additional data using this load_new_data function in the book-store
load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates;

# COMMAND ----------


