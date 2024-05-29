import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("amit_spark_de")\
        .config("spark.driver.extraClassPath", r"C:\Users\Rupali\PycharmProjects\youtube_de_project1\local\drivers\mysql-connector-j-8.0.33\mysql-connector-j-8.0.33.jar") \
        .getOrCreate()
    logger.info("spark session %s", spark)
    return spark