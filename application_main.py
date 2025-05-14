import sys, logging
from lib import dataManipulation, dataReader, utils
from pyspark.sql.functions import *
'''from utils import setup_logging
setup_logging()
logger = logging.getLogger(__name__)'''

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    print("Creating Spark Session")
    spark = utils.get_spark_session(job_run_env)
    print("Created Spark Session")
    customers_df = dataReader.read_customers(spark,job_run_env)
    #logger.info("Loading data...")

    # Set rating thresholds as Spark config
    spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
    spark.conf.set("spark.sql.very_bad_rated_pts", 100)
    spark.conf.set("spark.sql.bad_rated_pts", 250)
    spark.conf.set("spark.sql.good_rated_pts", 500)
    spark.conf.set("spark.sql.very_good_rated_pts", 650)
    spark.conf.set("spark.sql.excellent_rated_pts", 800)

    spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
    spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
    spark.conf.set("spark.sql.bad_grade_pts", 1500)
    spark.conf.set("spark.sql.good_grade_pts", 2000)
    spark.conf.set("spark.sql.very_good_grade_pts", 2500)
    
    # 1. append memberid column in customers data
    new_df = dataManipulation.generate_member_id(customers_df)
    
    # 2. create view from new_df
    dataManipulation.create_view(new_df,"customers_raw",spark)
    
    # 3. create customers_data_df
    customers_data_df = dataManipulation.create_customers_data_df("customers_raw",spark)
        
    # 4. create loans_data_df
    loans_data_df = dataManipulation.create_loans_data_df("customers_raw",spark)
    
    # 5. create loans_repayment_df
    loans_repayment_df = dataManipulation.create_loans_repayment_df("customers_raw",spark)

    # 6. create loans_defaulter_df
    loans_defaulter_df = dataManipulation.create_loans_defaulter_df("customers_raw",spark)
    #logger.info("DataFrame creation completed")

    # 7. clean customers data
    customers_data_final = dataManipulation.clean_customers_data(customers_data_df,spark)
    
    # 8. clean loans data
    loans_data_final = dataManipulation.clean_loans_data(loans_data_df,spark)

    # 9. clean loans repayment data
    loans_repayment_final = dataManipulation.clean_loans_repayment_data(loans_repayment_df,spark)
    
    # 10. clean loans defaulter data
    loans_def_delinq_df, loans_def_detail_records_enq_df = dataManipulation.clean_loans_defaulter_data(loans_defaulter_df,spark)
    #logger.info("Data cleaning completed")

    # 11. Create loans verification data table
    customers_loan_v_df = dataManipulation.create_loans_verification_df (customers_data_final,loans_data_final,loans_repayment_final,loans_def_delinq_df, loans_def_detail_records_enq_df, spark)

    # 12. Create bad customers data
    bad_customer_data_final_df = dataManipulation.create_bad_customer_df(customers_loan_v_df,loans_def_delinq_df,loans_def_detail_records_enq_df,spark)
  
    # 13. Filter out bad customer data from customers_data_final
    customers_df = dataManipulation.create_customers_df(customers_data_final,bad_customer_data_final_df,spark)
   
    # 14. Create customer loan score
    loan_score_final = dataManipulation.create_loan_score(spark,customers_df,loans_data_final,loans_repayment_final,bad_customer_data_final_df,loans_def_delinq_df, loans_def_detail_records_enq_df)
    #logger.info("Training model completed.")
    loan_score_final.printSchema()
    loan_score_final.show()
    print("end of main")

spark.stop()
print("spark session stopped")