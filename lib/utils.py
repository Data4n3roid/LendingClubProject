from pyspark.sql import SparkSession
from lib.configReader import get_pyspark_config

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .master("local[2]") \
        .getOrCreate()
    else:
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .enableHiveSupport() \
        .getOrCreate()
    
def set_spark_config(spark):
    spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
    spark.conf.set("spark.sql.very_bad_rated_pts", 100)
    spark.conf.set("spark.sql.bad_rated_pts", 250)
    spark.conf.set("spark.sql.good_rated_pts", 500)
    spark.conf.set("spark.sql.very_good_rated_pts", 650)
    spark.conf.set("spark.sql.excellent_rated_pts", 800)
    spark.conf.set("spark.sql.unacceptable_grade_pts", 200)

'''import logging

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("lendingclub_pipeline.log"),
            logging.StreamHandler()
        ]
    )'''