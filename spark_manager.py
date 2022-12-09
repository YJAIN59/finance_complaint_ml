from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('abc').getOrCreate()