import os

MASTER_CODE_DIR = os.getcwd()
data_dir = os.path.join(MASTER_CODE_DIR, 'data_files')
json_data_dir = os.path.join(data_dir, "json_data")
parquet_data_dir = os.path.join(data_dir, "parquet_data")

test_size = 0.8
# print(data_dir_path)