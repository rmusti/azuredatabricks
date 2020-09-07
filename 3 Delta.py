# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %fs ls /mnt/training/movies/20m/

# COMMAND ----------

RATINGS_CSV_FILE = "dbfs:/mnt/training/movies/20m/ratings.csv"
MOVIES_CSV_FILE = "dbfs:/mnt/training/movies/20m/movies.csv"

RATINGS_CSV_df = spark.read.option("header",True).option("inferSchema",True).csv(RATINGS_CSV_FILE)
MOVIES_CSV_df = spark.read.option("header",True).option("inferSchema",True).csv(MOVIES_CSV_FILE)

# COMMAND ----------

RATINGS_CSV_df.show()

# COMMAND ----------

MOVIES_CSV_df.show()

# COMMAND ----------

MoviesDeltaPath = "dbfs:/mnt/sqlondemand1/delta/movies/movie/"
RatingsDeltaPath = "dbfs:/mnt/sqlondemand1/delta/movies/ratings/"
MOVIES_CSV_df.write.mode("overwrite").format("delta").save(MoviesDeltaPath)
RATINGS_CSV_df.write.mode("overwrite").format("delta").save(RatingsDeltaPath)

# COMMAND ----------

display(spark.read.format("delta").load(MoviesDeltaPath).limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE movies_delta 
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/mnt/sqlondemand1/delta/movies/movie/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ratings_delta
# MAGIC USINg DELTA
# MAGIC LOCATION "dbfs:/mnt/sqlondemand1/delta/movies/ratings/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM movies_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ratings_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See which movies got highest number of user ratings and what is the avg ratings
# MAGIC SELECT m.movieID, m.title , count(r.userID) as usersRated , avg(r.rating) as avgRating
# MAGIC FROM movies_delta m JOIN ratings_delta r ON m.movieId = r.movieID
# MAGIC GROUP BY m.movieId , m.title
# MAGIC ORDER BY usersRated DESC
# MAGIC LIMIT 10

# COMMAND ----------

