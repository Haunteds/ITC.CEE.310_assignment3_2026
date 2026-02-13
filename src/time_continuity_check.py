# For bronze table
bronze_dates = spark.read.table("mlops_course.ingesting_data.bronze_table").select("date").distinct()
bronze_dates = bronze_dates.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
bronze_min = bronze_dates.agg({"date": "min"}).collect()[0][0]
bronze_max = bronze_dates.agg({"date": "max"}).collect()[0][0]
bronze_count = bronze_dates.count()

# For silver table
silver_dates = spark.read.table("mlops_course.ingesting_data.silver_table").select("date").distinct()
silver_min = silver_dates.agg({"date": "min"}).collect()[0][0]
silver_max = silver_dates.agg({"date": "max"}).collect()[0][0]
silver_count = silver_dates.count()

# Generate expected date range
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
import pandas as pd

date_range = pd.date_range(start=bronze_min, end=bronze_max)
expected_count = len(date_range)

print(f"Bronze: {bronze_count} unique dates, Silver: {silver_count} unique dates, Expected: {expected_count}")

# Find missing dates in silver
bronze_dates_list = [row.date for row in bronze_dates.collect()]
silver_dates_list = [row.date for row in silver_dates.collect()]
missing_dates = set(bronze_dates_list) - set(silver_dates_list)
print("Missing dates in silver:", missing_dates)