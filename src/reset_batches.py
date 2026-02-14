import os
import time
import shutil

target_dir = "/Volumes/mlops_course/ingesting_data/raw_batch_files/"

# Remove all contents from target_dir (Delta table location)
for item in os.listdir(target_dir):
    item_path = os.path.join(target_dir, item)
    if os.path.isdir(item_path):
        shutil.rmtree(item_path)
    else:
        os.remove(item_path)