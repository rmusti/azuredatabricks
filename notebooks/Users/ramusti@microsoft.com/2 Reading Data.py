# Databricks notebook source
# MAGIC %fs ls /mnt/training/movies/20m/

# COMMAND ----------

# MAGIC %fs head /mnt/training/movies/20m/ratings.csv

# COMMAND ----------

CSV_FILE = "dbfs:/mnt/training/movies/20m/ratings.csv"

temp_df = spark.read.csv(CSV_FILE)
temp_df.printSchema()

# COMMAND ----------

(spark.read
 .option("header",True)
 .option("inferSchema",True)
 .csv(CSV_FILE)
 .printSchema())

# COMMAND ----------

ratings_df = (spark.read
 .option("header",True)
 .option("inferSchema",True)
 .csv(CSV_FILE))

# COMMAND ----------

ratings_df.show()

# COMMAND ----------

display(ratings_df)

# COMMAND ----------

ratings_df.write.saveAsTable("ratings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MOVIES
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM RATINGS
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.userId, m.title, r.rating FROM RATINGS r JOIN MOVIES m 
# MAGIC ON r.movieId = m.movieId
# MAGIC WHERE userID = 2
# MAGIC ORDER BY r.rating DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT userId,  avg(rating) as avgRating FROM RATINGS 
# MAGIC GROUP BY userID
# MAGIC ORDER BY userID ASC

# COMMAND ----------

