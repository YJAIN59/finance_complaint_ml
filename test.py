from logger import logger
from pyspark.sql import DataFrame
from spark_manager import spark_session

import data_ingestion

if __name__ == "__main__":
    dataframe: DataFrame = spark_session.read.parquet('data_files/parquet_data/finance_complaint')
    print(dataframe.head())
    logger.info('yetsgv yash test loggg')
