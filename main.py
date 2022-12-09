from data_ingestion import download_files, download_prep,convert_files_to_parquet
from data_transformation import DataTransformation

if __name__ == "__main__":
    # url = download_prep()
    # download_files(download_url=url)
    # parquet_file_path = (convert_files_to_parquet())
    parquet_file_path = 'data_files/parquet_data/finance_complaint'

    obj = DataTransformation()
    obj.run_transformation(parquet_file_path)
