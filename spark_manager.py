from pyspark.sql import SparkSession
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark_session = SparkSession.builder.appName('abc').getOrCreate()