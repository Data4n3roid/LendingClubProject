from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from functools import reduce


# Create or replace a temporary Spark SQL view from a DataFrame.
def create_view(df,new_view,spark):
    df.createOrReplaceTempView(new_view)
    print(f"Temporary view '{new_view}' created. Preview:")
    #spark.sql(f"SELECT * FROM {new_view}").show(5)

# Generate unique 32 character member id using concatenation of multiple columns.
def generate_member_id(customers_df):
    df_with_id = customers_df.withColumn("member_id", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))
    return df_with_id

# Create a DataFrame with customers_data.
def create_customers_data_df(raw_view,spark):
    customers_data_df = spark.sql(f"""
    SELECT member_id, emp_title, emp_length, home_ownership, annual_inc,
           addr_state, zip_code, 'USA' AS country, grade, sub_grade,
           verification_status, tot_hi_cred_lim, application_type,
           annual_inc_joint, verification_status_joint
    FROM {raw_view}
    """)
    return customers_data_df

# Create a DataFrame with loans_data.
def create_loans_data_df(raw_view,spark):
    loans_data_df = spark.sql(f"""
    SELECT id as loan_id, member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,title 
    FROM {raw_view}
    """)
    return loans_data_df

# Create a DataFrame with loans_repayment_data.
def create_loans_repayment_df(raw_view,spark):
    loans_repayment_df = spark.sql(f"""
    select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d 
    FROM {raw_view}
    """)
    return loans_repayment_df

# Create a DataFrame with loans_defaulter_data.
def create_loans_defaulter_df(raw_view,spark):
    loans_defaulter_df = spark.sql(f"""
    select member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record 
    FROM {raw_view}
    """)
    return loans_defaulter_df

# Clean the customers data 

# Define the schema mapping for the customers data
def get_customers_schema_mapping():
    return {
        "member_id": ("member_id", StringType()),
        "emp_title": ("emp_title", StringType()),
        "emp_length": ("emp_length", StringType()),
        "home_ownership": ("home_ownership", StringType()),
        "annual_inc": ("annual_income", FloatType()),
        "addr_state": ("address_state", StringType()),
        "zip_code": ("address_zipcode", StringType()),
        "country": ("address_country", StringType()),
        "grade": ("grade", StringType()),
        "sub_grade": ("sub_grade", StringType()),
        "verification_status": ("verification_status", StringType()),
        "tot_hi_cred_lim": ("total_high_credit_limit", FloatType()),
        "application_type": ("application_type", StringType()),
        "annual_inc_joint": ("joint_annual_income", FloatType()),
        "verification_status_joint": ("verification_status_joint", StringType()),
    }

# Update the column names and type
def clean_customers_data(customers_data_df, spark):
    schema_mapping = get_customers_schema_mapping()
    
    transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in schema_mapping.items()
    ]
    customers_data_df_renamed = customers_data_df.select(*transformed_columns)

    # add a column to record data ingestion date. Insert current timestamp for now.
    customers_df_ingested = customers_data_df_renamed.withColumn("ingest_date", current_timestamp())
    
    # remove duplicate records
    customers_distinct = customers_df_ingested.distinct()
    
    # remove all records where annual income is null
    customers_income_filtered = customers_distinct.filter(customers_distinct.annual_income.isNotNull())

    # Clean the emp_length column values to have only numbers
    customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)",""))

    # Cast emp_length to int
    customers_emplength_casted = customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('int'))

    # Fill all null emp_lenght values with average
    avg_emp_length = customers_emplength_casted.select(floor(avg("emp_length"))).collect()[0][0]
    avg_emp_duration = avg_emp_length
    customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=['emp_length'])

    # Clean up state codes to have 2 letter codes only
    customers_data_final = customers_emplength_replaced.withColumn("address_state",
    when(length(col("address_state"))> 2, "NA").otherwise(col("address_state")))
    return customers_data_final



# Clean the loans data

# Define the schema mapping for the loans data
def get_loans_schema_mapping():
    return {
        "loan_id": ("loan_id", StringType()),
        "member_id": ("member_id", StringType()),
        "loan_amnt": ("loan_amount", FloatType()),
        "funded_amnt": ("funded_amount", FloatType()),
        "term": ("loan_term_months", StringType()),
        "int_rate": ("interest_rate", FloatType()),
        "installment": ("monthly_installment", FloatType()),
        "issue_d": ("issue_date", StringType()),
        "loan_status": ("loan_status", StringType()),
        "purpose": ("loan_purpose", StringType()),
        "title": ("loan_title", StringType()),
    }

# Update the column names and type
def clean_loans_data(loans_data_df, spark):
    loans_schema_mapping = get_loans_schema_mapping()
    
    loans_transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in loans_schema_mapping.items()
    ]
    loans_data_df_renamed = loans_data_df.select(*loans_transformed_columns)

    # add a column to record data ingestion date. Insert current timestamp for now.
    loans_df_ingested = loans_data_df_renamed.withColumn("ingest_date", current_timestamp())
    
    # remove records if any of the important column has null value
    columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    loans_filtered_df = loans_df_ingested.na.drop(subset=columns_to_check)
    
    # update loan term years column to show loan term in months
    loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
    .cast("int") / 12) \
    .cast("int")) \
    .withColumnRenamed("loan_term_months","loan_term_years")

    # Keep only relevant loan purpose values and move remaining to others
    loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]
    loans_data_final = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))
    return loans_data_final
   

# Clean the loans repayment data

# Define the schema mapping for the loans repayment data
def get_loans_repay_schema_mapping():
    return {
        "loan_id": ("loan_id", StringType()),
        "total_rec_prncp": ("total_principal_received", FloatType()),
        "total_rec_int": ("total_interest_received", FloatType()),
        "total_rec_late_fee": ("total_late_fee_received", FloatType()),
        "total_pymnt": ("total_payment_received", FloatType()),
        "last_pymnt_amnt": ("last_payment_amount", FloatType()),
        "last_pymnt_d": ("last_payment_date", StringType()),
        "next_pymnt_d": ("next_payment_date", StringType()),
    }

def clean_loans_repayment_data(loans_repayment_data_df, spark):
    # Update the column names and type
    loans_repay_schema_mapping = get_loans_repay_schema_mapping()
    
    loans_repay_transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in loans_repay_schema_mapping.items()
    ]
    loans_repay_data_df_renamed = loans_repayment_data_df.select(*loans_repay_transformed_columns)

    # add a column to record data ingestion date. Insert current timestamp for now.
    loans_repay_df_ingested = loans_repay_data_df_renamed.withColumn("ingest_date", current_timestamp())
    
    # remove records if any of the important column has null value
    repay_columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]
    loans_repay_filtered_df = loans_repay_df_ingested.na.drop(subset=repay_columns_to_check)
    
    # update total_payments_received column if it shows 0 and sub columns have data
    loans_repay_fixed_df = loans_repay_filtered_df.withColumn("total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
    )

    # Drop records where total repayment received still shows 0
    loans_repay_fixed2_df = loans_repay_fixed_df.filter("total_payment_received != 0.0")

    # Fix last payment date
    loans_repay_ldate_fixed_df = loans_repay_fixed2_df.withColumn("last_payment_date",
    when(
       (col("last_payment_date") == 0.0),
       None
       ).otherwise(col("last_payment_date"))
    )   

    # Fix next payment date
    loans_repayment_final = loans_repay_ldate_fixed_df.withColumn("last_payment_date",
    when(
       (col("next_payment_date") == 0.0),
       None
       ).otherwise(col("next_payment_date"))
    )
    return loans_repayment_final


# Clean the loans defaulter data

# Define the schema mapping for the loans defaulter data
def get_loans_defaulter_schema_mapping():
    return {
        "member_id": ("member_id", StringType()),
        "delinq_2yrs": ("delinq_2yrs", FloatType()),
        "delinq_amnt": ("delinq_amnt", FloatType()),
        "pub_rec": ("pub_rec", FloatType()),
        "pub_rec_bankruptcies": ("pub_rec_bankruptcies", FloatType()),
        "inq_last_6mths": ("inq_last_6mths", FloatType()),
        "total_rec_late_fee": ("total_rec_late_fee", FloatType()),
        "mths_since_last_delinq": ("mths_since_last_delinq", FloatType()),
        "mths_since_last_record": ("mths_since_last_record", FloatType()),
    }

def clean_loans_defaulter_data(loans_defaulter_data_df, spark):
    # Update the column names and type
    loans_defaulter_schema_mapping = get_loans_defaulter_schema_mapping()
    
    loans_defaulter_transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in loans_defaulter_schema_mapping.items()
    ]
    loans_defaulter_data_df_renamed = loans_defaulter_data_df.select(*loans_defaulter_transformed_columns)

    # Update column delinq_2years with 0 for null.
    loans_defaulters_processed_df = loans_defaulter_data_df_renamed.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])

    # Create a df of defaults
    loans_def_delinq_df = loans_defaulters_processed_df \
    .withColumn("mths_since_last_delinq", col("mths_since_last_delinq").cast("int")) \
    .filter((col("delinq_2yrs") > 0) | (col("mths_since_last_delinq") > 0)) \
    .select("member_id", "delinq_2yrs", "delinq_amnt", "mths_since_last_delinq")

    """# Create a df of enquiries
    loans_def_records_enq_df = loans_defaulters_processed_df.filter(
    (col("pub_rec") > 0.0) |
    (col("pub_rec_bankruptcies") > 0.0) |
    (col("inq_last_6mths") > 0.0)
    ).select("member_id")"""

    # Create detailed file of enquiries
    loans_def_p_pub_rec_df = loans_defaulters_processed_df.withColumn("pub_rec", col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"])
    loans_def_p_pub_rec_bankruptcies_df = loans_def_p_pub_rec_df.withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"])
    loans_def_p_inq_last_6mths_df = loans_def_p_pub_rec_bankruptcies_df.withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"])
    loans_def_detail_records_enq_df = loans_def_p_inq_last_6mths_df.select(col("member_id"), col("pub_rec"), col("pub_rec_bankruptcies"), col("inq_last_6mths"))
    
    return loans_def_delinq_df, loans_def_detail_records_enq_df


# Create final joined table out of all the above tables
def create_loans_verification_df(customers_data_final,loans_data_final,loans_repayment_final,loans_def_delinq_df, loans_def_detail_records_enq_df, spark):
    customers_loan_v_df = (
    customers_data_final.alias("c")
    .join(loans_data_final.alias("l"), col("c.member_id") == col("l.member_id"), "left")
    .join(loans_repayment_final.alias("r"), col("l.loan_id") == col("r.loan_id"), "left")
    .join(loans_def_delinq_df.alias("d"), col("c.member_id") == col("d.member_id"), "left")
    .join(loans_def_detail_records_enq_df.alias("e"), col("c.member_id") == col("e.member_id"), "left")
    .select(
        col("l.loan_id"),
        col("c.member_id"),
        col("c.emp_title"),
        col("c.emp_length"),
        col("c.home_ownership"),
        col("c.annual_income"),
        col("c.address_state"),
        col("c.address_zipcode"),
        col("c.address_country"),
        col("c.grade"),
        col("c.sub_grade"),
        col("c.verification_status"),
        col("c.total_high_credit_limit"),
        col("c.application_type"),
        col("c.joint_annual_income"),
        col("c.verification_status_joint"),
        col("l.loan_amount"),
        col("l.funded_amount"),
        col("l.loan_term_years"),
        col("l.interest_rate"),
        col("l.monthly_installment"),
        col("l.issue_date"),
        col("l.loan_status"),
        col("l.loan_purpose"),
        col("r.total_principal_received"),
        col("r.total_interest_received"),
        col("r.total_late_fee_received"),
        col("r.last_payment_date"),
        col("r.next_payment_date"),
        col("d.delinq_2yrs"),
        col("d.delinq_amnt"),
        col("d.mths_since_last_delinq"),
        col("e.pub_rec"),
        col("e.pub_rec_bankruptcies"),
        col("e.inq_last_6mths")
    )
    )
    return customers_loan_v_df

# Create bad customers data table

# Create bad customers df
def create_bad_customer_df(customers_data_final,loans_def_delinq_df,loans_def_detail_records_enq_df,spark):
    bad_data_customer_df = (
    customers_data_final
    .groupBy("member_id")
    .agg(count("*").alias("total"))
    .filter(col("total") > 1)
    .select("member_id")
    )
    
    bad_data_loans_defaulters_delinq_df = (
    loans_def_delinq_df
    .groupBy("member_id")
    .agg(count("*").alias("total"))
    .filter(col("total") > 1)
    .select("member_id")
    )

    bad_data_loans_defaulters_detail_rec_enq_df = (
    loans_def_detail_records_enq_df
    .groupBy("member_id")
    .agg(count("*").alias("total"))
    .filter(col("total") > 1)
    .select("member_id")
    )
    
    bad_customer_data_df = bad_data_customer_df.select("member_id") \
    .union(bad_data_loans_defaulters_delinq_df.select("member_id")) \
    .union(bad_data_loans_defaulters_detail_rec_enq_df.select("member_id"))

    bad_customer_data_final_df = bad_customer_data_df.distinct()
    return bad_customer_data_final_df

def create_customers_df(customers_data_final,bad_customer_data_final_df,spark):
    customers_df = customers_data_final.join(bad_customer_data_final_df, on="member_id", how="left_anti")
    return customers_df


def create_loan_score(
    spark,
    customers_df,
    loans_data_final,
    loans_repayment_final,
    loans_def_detail_records_enq_df,
    loans_def_delinq_df,
    bad_customer_data_final_df
):
    from pyspark.sql.functions import col, when, lower

    excellent_pts = int(spark.conf.get("spark.sql.excellent_rated_pts"))
    very_good_pts = int(spark.conf.get("spark.sql.very_good_rated_pts"))
    good_pts = int(spark.conf.get("spark.sql.good_rated_pts"))
    bad_pts = int(spark.conf.get("spark.sql.bad_rated_pts"))
    very_bad_pts = int(spark.conf.get("spark.sql.very_bad_rated_pts"))
    unacceptable_pts = int(spark.conf.get("spark.sql.unacceptable_rated_pts"))

    # Step 1: Score repayment data
    loans_repayment_scored = loans_repayment_final.alias("p") \
        .join(loans_data_final.select("loan_id", "monthly_installment", "funded_amount").alias("l"), col("p.loan_id") == col("l.loan_id")) \
        .withColumn("last_payment_pts", when(col("last_payment_amount") < (col("monthly_installment") * 0.5), very_bad_pts)
            .when((col("last_payment_amount") >= (col("monthly_installment") * 0.5)) & (col("last_payment_amount") < col("monthly_installment")), very_bad_pts)
            .when(col("last_payment_amount") == col("monthly_installment"), good_pts)
            .when((col("last_payment_amount") > col("monthly_installment")) & (col("last_payment_amount") <= (col("monthly_installment") * 1.5)), very_good_pts)
            .when(col("last_payment_amount") > (col("monthly_installment") * 1.5), excellent_pts)
            .otherwise(unacceptable_pts)) \
        .withColumn("total_payment_pts", when(col("total_payment_received") >= (col("funded_amount") * 0.5), very_good_pts)
            .when((col("total_payment_received") < (col("funded_amount") * 0.5)) & (col("total_payment_received") > 0), good_pts)
            .when((col("total_payment_received") == 0) | (col("total_payment_received").isNull()), unacceptable_pts)
            .otherwise(unacceptable_pts)) \
        .select("p.loan_id", "last_payment_pts", "total_payment_pts")

    repayment_with_member = loans_repayment_scored.alias("p") \
        .join(loans_data_final.select("loan_id", "member_id").alias("ld"), col("p.loan_id") == col("ld.loan_id"), "inner") \
        .select(col("ld.member_id"), "last_payment_pts", "total_payment_pts")

    # Step 2: Construct main DF
    fh_ldh_ph_df = loans_def_detail_records_enq_df.alias("l") \
        .join(loans_def_delinq_df.alias("d"), col("l.member_id") == col("d.member_id"), "inner") \
        .join(repayment_with_member.alias("p"), col("l.member_id") == col("p.member_id"), "inner") \
        .join(loans_data_final.alias("loans"), col("l.member_id") == col("loans.member_id"), "inner") \
        .join(customers_df.alias("a"), col("l.member_id") == col("a.member_id"), "inner") \
        .join(bad_customer_data_final_df.alias("bad"), col("l.member_id") == col("bad.member_id"), "left_anti") \
        .select(
            col("l.member_id"),
            col("p.last_payment_pts"), col("p.total_payment_pts"),
            col("d.delinq_2yrs").cast("int").alias("delinq_pts"),
            col("l.pub_rec").alias("public_records_pts"),
            col("l.pub_rec_bankruptcies").alias("public_bankruptcies_pts"),
            col("l.inq_last_6mths").alias("enq_pts"),
            col("loans.loan_status"), col("a.home_ownership"), col("a.total_high_credit_limit"),
            col("loans.funded_amount"), col("a.grade"), col("a.sub_grade")
        )

    loan_score = fh_ldh_ph_df.select(
        col("member_id"),
        ((col("last_payment_pts") + col("total_payment_pts")) * 0.20).alias("payment_history_pts"),
        ((col("delinq_pts") + col("public_records_pts") + col("public_bankruptcies_pts") + col("enq_pts")) * 0.45).alias("defaulters_history_pts"),
        ((when(lower(col("loan_status")).like("%fully paid%"), excellent_pts)
         .when(lower(col("loan_status")).like("%current%"), good_pts)
         .when(lower(col("loan_status")).like("%in grace period%"), bad_pts)
         .when(lower(col("loan_status")).like("%late%"), very_bad_pts)
         .when(lower(col("loan_status")).like("%charged off%"), unacceptable_pts)
         .otherwise(unacceptable_pts))
         + (when(lower(col("home_ownership")).like("%own"), excellent_pts)
            .when(lower(col("home_ownership")).like("%rent"), good_pts)
            .when(lower(col("home_ownership")).like("%mortgage"), bad_pts)
            .otherwise(very_bad_pts))
         + (when(col("funded_amount") <= col("total_high_credit_limit") * 0.10, excellent_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.20, very_good_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.30, good_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.50, bad_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.70, very_bad_pts)
            .otherwise(unacceptable_pts))
         + (when(col("grade") == 'A', excellent_pts)
            .when(col("grade") == 'B', very_good_pts)
            .when(col("grade") == 'C', good_pts)
            .when(col("grade") == 'D', bad_pts)
            .when(col("grade") == 'E', very_bad_pts)
            .otherwise(unacceptable_pts))) * 0.35).alias("financial_health_pts")
    

    final_loan_score = loan_score.withColumn(
        'loan_score',
        col("payment_history_pts") + col("defaulters_history_pts") + col("financial_health_pts")
    )

    final_loan_score.createOrReplaceTempView("loan_score_eval")

    loan_score_final = spark.sql(f"""
        SELECT ls.*, 
        CASE 
            WHEN loan_score > {spark.conf.get("spark.sql.very_good_grade_pts")} THEN 'A'
            WHEN loan_score <= {spark.conf.get("spark.sql.very_good_grade_pts")} AND loan_score > {spark.conf.get("spark.sql.good_grade_pts")} THEN 'B'
            WHEN loan_score <= {spark.conf.get("spark.sql.good_grade_pts")} AND loan_score > {spark.conf.get("spark.sql.bad_grade_pts")} THEN 'C'
            WHEN loan_score <= {spark.conf.get("spark.sql.bad_grade_pts")} AND loan_score > {spark.conf.get("spark.sql.very_bad_grade_pts")} THEN 'D'
            WHEN loan_score <= {spark.conf.get("spark.sql.very_bad_grade_pts")} AND loan_score > {spark.conf.get("spark.sql.unacceptable_grade_pts")} THEN 'E'
            WHEN loan_score <= {spark.conf.get("spark.sql.unacceptable_grade_pts")} THEN 'F'
        END AS loan_final_grade
        FROM loan_score_eval ls
    """)

    return loan_score_final

