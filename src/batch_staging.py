import os
import shutil
import time

source_dir = "/Volumes/mlops_course/ingesting_data/staging_batch_files/"
target_dir = "/Volumes/mlops_course/ingesting_data/raw_batch_files/"
interval_seconds = 60

batches = sorted([d for d in os.listdir(source_dir) if d.startswith("batch")])

# Remove all contents from target_dir
for item in os.listdir(target_dir):
    item_path = os.path.join(target_dir, item)
    if os.path.isdir(item_path):
        shutil.rmtree(item_path)
    else:
        os.remove(item_path)


for batch in batches:
    src_batch = os.path.join(source_dir, batch)
    dst_batch = os.path.join(target_dir, batch)
    if not os.path.exists(dst_batch):
        shutil.copytree(src_batch, dst_batch)
        print(f"Copied {batch} to target directory.")
        time.sleep(interval_seconds)