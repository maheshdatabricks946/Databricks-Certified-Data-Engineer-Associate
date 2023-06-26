# Databricks notebook source
# MAGIC %md
# MAGIC ### 3.1 - Structured Streaming

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/mnt/demo-datasets/bookstore/books-csv/export_001.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS books_vw;
# MAGIC DROP TABLE IF EXISTS books;
# MAGIC
# MAGIC CREATE TEMP VIEW  books_vw 
# MAGIC (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC     path = "dbfs:/mnt/demo-datasets/bookstore/books-csv/",
# MAGIC     header = "true",
# MAGIC     delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS books
# MAGIC AS SELECT * FROM books_vw;
# MAGIC
# MAGIC SELECT * FROM books LIMIT 10;

# COMMAND ----------

(spark.readStream
 .table("books")
 .createOrReplaceTempView("books_streaming_tmp_vw")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw
# MAGIC ORDER BY author;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

(
    spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(processingTime= '4 seconds')
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
    .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES ("V19", "Intro to Model and Simulation", "Wilson david", "Computer Science", 25),
# MAGIC ("V20", "Robort Model and Control", "Wilson david", "Computer Science", 30),
# MAGIC ("V21", "Turing's Vision: The Birth of Comuter Science ", "Wilson david", "Computer Science", 35)

# COMMAND ----------

(
    spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("complete")
    .option("checkpointLocation","dbfs:/mnt/demo/author_counts_checkpoint")
    .table("author_counts")
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------


