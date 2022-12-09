from pyspark.sql import DataFrame
from spark_manager import spark_session
import os
from config import *
import logger

test_size = test_size
class DataTransformation:
    def __init__(self) -> None:
        pass

    def read_data(self) -> DataFrame:
        try:
            file_path = ''
            for _file_name in os.listdir(data_dir_path):
                if '.parquet' in _file_name:
                    file_path = os.path.join(data_dir_path, _file_name)
                    break
            print(file_path)
            dataframe: DataFrame = spark_session.read.parquet(file_path)
            dataframe.printSchema()
            print("\n\n")
            print(dataframe.head())
            return dataframe
        except Exception as e:
            print("into exe   . . . . .   ..  ")
            raise e


    def run_transformation(self):
        # Step1- read data
        dataframe = self.read_data()

        #Step-2 Split Data
        logger.info(f"Splitting dataset into train and test set using ration: {1 - test_size}:{test_size}")
        train_dataframe, test_dataframe = dataframe.randomSplit([1 - test_size, test_size])

        logger.info(f"Training Dataset = {train_dataframe.head()}")
        logger.info(f"Testing Dataset = {test_dataframe.head()}")



if __name__ == "__main__":
    obj = DataTransformation()
    obj.run_transformation()




