# The pipeline source file
from pyspark import pipelines
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Get pipeline configs
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")

source_path = f'/Volumes/{catalog_name}/{schema_name}/raw_batch_files/'

# Bronze table
# Read the CSV data from the source location using Auto Loader into a bronze-level table
# The batches are read incrementally by the Auto Loader but in random order.
@pipelines.table
def bronze_table():
    return (
        spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true")
        .load(source_path)
        .withColumn("date", col("date").cast("string"))
    )


# Silver table
@pipelines.table

# Checking if the values are within the expected range
@pipelines.expect("valid_temperature", "meantemp BETWEEN -80 AND 60")
@pipelines.expect("valid_humidity", "humidity BETWEEN 0 AND 100")
@pipelines.expect("valid_wind_speed", "wind_speed BETWEEN 0 AND 120")
@pipelines.expect("valid_pressure", "meanpressure BETWEEN 900 AND 1100")

# Checking for null values in any of the columns and dropping them
@pipelines.expect_or_drop("no_nulls", "date IS NOT NULL AND \
                        meantemp IS NOT NULL AND \
                        humidity IS NOT NULL AND \
                        wind_speed IS NOT NULL AND \
                        meanpressure IS NOT NULL")


def silver_table():
    return (
        spark.readStream.table("bronze_table") \
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("meantemp", round(col("meantemp"), 6)) \
        .withColumn("humidity", round(col("humidity"), 6)) \
        .withColumn("wind_speed", round(col("wind_speed"), 6)) \
        .withColumn("meanpressure", round(col("meanpressure"), 6)) \
        .dropDuplicates(["date"]) \
        .select("date", "meantemp", "humidity", "wind_speed", "meanpressure")
    )


# Gold table
# I have added a new column meantemp_next_day which is the next day's mean temperature
@pipelines.materialized_view()
def gold_table():
    return (
        spark.read.table("silver_table") \
        .withColumn("meantemp_next_day", lead("meantemp", 1).over(Window.orderBy("date"))) \
        .select("date", "meantemp", "meantemp_next_day", "humidity", "wind_speed", "meanpressure")        
    )
