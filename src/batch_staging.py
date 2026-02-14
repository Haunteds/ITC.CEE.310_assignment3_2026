import os
import time
import shutil

source_dir = "/Volumes/mlops_course/ingesting_data/stage_batch_files/"
target_dir = "/Volumes/mlops_course/ingesting_data/raw_batch_files/"
interval_seconds = 10

batches = sorted([d for d in os.listdir(source_dir) if d.startswith("batch")])

delta_path = target_dir  # Delta table location
first_batch = True

for batch in batches:
    src_batch = os.path.join(source_dir, batch)
    csv_files = [f for f in os.listdir(src_batch) if f.endswith('.csv')]
    for csv_file in csv_files:
        src_file = os.path.join(src_batch, csv_file)
        df = spark.read.option("header", True).csv(src_file)
        if first_batch:
            df.write.format("delta").mode("overwrite").save(delta_path)
            print(f"Created Delta table with {csv_file} from {batch}.")
            first_batch = False
        else:
            df.write.format("delta").mode("append").save(delta_path)
            print(f"Appended {csv_file} from {batch} to Delta table.")
    time.sleep(interval_seconds)

