from pyspark.sql import DataFrame
from spark_manager import spark_session
import os
from config import *
from logger import logger

test_size = test_size
class DataTransformation:
    def __init__(self) -> None:
        pass

    def read_data(self, parquet_file_path) -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(parquet_file_path)
            dataframe.printSchema()
            print("\n\n")
            print(dataframe.head())
            return dataframe
        except Exception as e:
            print("into exe   . . . . .   ..  ")
            raise e

    def run_transformation(self, parquet_file_path):
        # Step1- read data
        dataframe = self.read_data(parquet_file_path)

        #Step-2 Split Data
        logger.info(f"Splitting dataset into train and test set using ration: {1 - test_size}:{test_size}")
        train_dataframe, test_dataframe = dataframe.randomSplit([1 - test_size, test_size])

        logger.info(f"Training Dataset = {train_dataframe.head()}")
        logger.info(f"Testing Dataset = {test_dataframe.head()}")



if __name__ == "__main__":
    parquet_file_path = 'data_files/parquet_data/finance_complaint'
    obj = DataTransformation()
    obj.run_transformation(parquet_file_path)




