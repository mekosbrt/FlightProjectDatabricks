# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/shared_uploads/mehmetberatkose@hotmail.com/rot.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/mehmetberatkose@hotmail.com/Airlines.csv

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

file_airlines = "dbfs:/FileStore/shared_uploads/mehmetberatkose@hotmail.com/Airlines.csv"


# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/shared_uploads/mehmetberatkose@hotmail.com/Airlines.csv

# COMMAND ----------

Airlines_schema = StructType([StructField('AirlineID', IntegerType(), True),
                     StructField('Name', StringType(), True),
                     StructField('IATA', StringType(), True),                  
                     StructField('Callsign', StringType(), True),      
                     StructField('Country', StringType(), True),
                     StructField('Active', BooleanType(), True),])

# COMMAND ----------

df_airl = spark.read.csv(file_airlines, header=True, schema=Airlines_schema)

# COMMAND ----------

df_airl.cache()

# COMMAND ----------

df_airl.count()

# COMMAND ----------

df_airl.printSchema()

# COMMAND ----------

display(df_airl.limit(5))

# COMMAND ----------

df_airlines = (spark.read.format("csv")
      .schema("AirlineID STRING,Name STRING ,Alias STRING,IATA STRING,ICAO STRING,Callsign STRING ,Country STRING,Active BOOLEAN")
      .option("header", "true")
      .option("path", "dbfs:/FileStore/shared_uploads/mehmetberatkose@hotmail.com/Airlines.csv")
      .load())

display(df_airlines)

# COMMAND ----------

df_routes = (spark.read.format("csv")
      .schema("Airline STRING,AirlineID STRING ,SourceAirport STRING,SourceAirportID STRING,DestinationAirport STRING,DestinationAirportID STRING ,Codeshare BOOLEAN, Stops INT, Equipment STRING")
      .option("header", "true")
      .option("path", "dbfs:/FileStore/shared_uploads/mehmetberatkose@hotmail.com/rot.csv")
      .load())

display(df_routes)

# COMMAND ----------

df_routes.createOrReplaceTempView("Flights_Route")

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE Flights_Route

# COMMAND ----------

spark.sql("SELECT count(AirlineID) as Sum,Airline FROM Flights_Route group by Airline order by count(AirlineID) desc limit 20").show()

# COMMAND ----------


