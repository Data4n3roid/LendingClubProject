from lib import configReader

# creating customers dataframe
def read_customers(spark,env):
    conf = configReader.get_app_config(env)
    customers_raw_file_path = conf["customers_raw.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(customers_raw_file_path)


