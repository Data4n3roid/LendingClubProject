from pyspark.sql.functions import (
    col, sha2, concat_ws, current_timestamp, regexp_replace, avg, floor, when, count, length, lower
)
from pyspark.sql.types import StringType, FloatType


# 1. Generate unique 32 character member id using concatenation of multiple columns.
def generate_member_id(customers_df):
    id_cols = [
        "emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code",
        "addr_state", "grade", "sub_grade", "verification_status"
    ]
    return customers_df.withColumn(
        "member_id",
        sha2(concat_ws("||", *id_cols), 256)
    )

# 2. Create or replace a temporary Spark SQL view from a DataFrame.
def create_view(df, new_view, spark):
    df.createOrReplaceTempView(new_view)
    print(f"Temporary view '{new_view}' created. Preview:")
    
# 3. Create a DataFrame with customers_data.
def create_customers_data_df(raw_view, spark):
    customers_data_df = spark.sql(f"""
        SELECT member_id, emp_title, emp_length, home_ownership, annual_inc,
               addr_state, zip_code, 'USA' AS country, grade, sub_grade,
               verification_status, tot_hi_cred_lim, application_type,
               annual_inc_joint, verification_status_joint
        FROM {raw_view}
    """)
    return customers_data_df

# 4. Create a DataFrame with loans_data.
def create_loans_data_df(raw_view, spark):
    loans_data_df = spark.sql(f"""
        SELECT id as loan_id, member_id, loan_amnt, funded_amnt, term, int_rate,
               installment, issue_d, loan_status, purpose, title
        FROM {raw_view}
    """)
    return loans_data_df

# 5. Create a DataFrame with loans_repayment_data.
def create_loans_repayment_df(raw_view, spark):
    loans_repayment_df = spark.sql(f"""
        SELECT id as loan_id, total_rec_prncp, total_rec_int, total_rec_late_fee,
               total_pymnt, last_pymnt_amnt, last_pymnt_d, next_pymnt_d
        FROM {raw_view}
    """)
    return loans_repayment_df

# 6. Create a DataFrame with loans_defaulter_data.
def create_loans_defaulter_df(raw_view, spark):
    loans_defaulter_df = spark.sql(f"""
        SELECT member_id, delinq_2yrs, delinq_amnt, pub_rec, pub_rec_bankruptcies,
               inq_last_6mths, total_rec_late_fee, mths_since_last_delinq, mths_since_last_record
        FROM {raw_view}
    """)
    return loans_defaulter_df

# 7. Define the schema mapping for the customers data and clean the customers data
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

def clean_customers_data(customers_data_df, spark):
    schema_mapping = get_customers_schema_mapping()
    transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in schema_mapping.items()
    ]
    df = customers_data_df.select(*transformed_columns)
    df = df.withColumn("ingest_date", current_timestamp())
    df = df.distinct()
    df = df.filter(col("annual_income").isNotNull())

    # Clean emp_length to only numbers, cast to int, fill nulls with average
    df = df.withColumn("emp_length", regexp_replace(col("emp_length"), r"\D", ""))
    df = df.withColumn("emp_length", col("emp_length").cast("int"))
    avg_emp_length = df.select(floor(avg("emp_length"))).collect()[0][0]
    df = df.na.fill(avg_emp_length, subset=['emp_length'])

    # Clean up state codes to have 2 letter codes only
    df = df.withColumn(
        "address_state",
        when(length(col("address_state")) > 2, "NA").otherwise(col("address_state"))
    )
    return df

# 8. Define the schema mapping for the loans data and clean it
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

def clean_loans_data(loans_data_df, spark):
    schema_mapping = get_loans_schema_mapping()
    transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in schema_mapping.items()
    ]
    df = loans_data_df.select(*transformed_columns)
    df = df.withColumn("ingest_date", current_timestamp())
    columns_to_check = [
        "loan_amount", "funded_amount", "loan_term_months", "interest_rate",
        "monthly_installment", "issue_date", "loan_status", "loan_purpose"
    ]
    df = df.na.drop(subset=columns_to_check)
    # Convert loan_term_months from "36 months" to years (int)
    df = df.withColumn(
        "loan_term_years",
        (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")
    ).drop("loan_term_months")
    # Standardize loan_purpose
    allowed_purposes = [
        "debt_consolidation", "credit_card", "home_improvement", "other",
        "major_purchase", "medical", "small_business", "car", "vacation",
        "moving", "house", "wedding", "renewable_energy", "educational"
    ]
    df = df.withColumn(
        "loan_purpose",
        when(col("loan_purpose").isin(allowed_purposes), col("loan_purpose")).otherwise("other")
    )
    return df

# 9. Define the schema mapping for the loans repayment data and clean it
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
    schema_mapping = get_loans_repay_schema_mapping()
    transformed_columns = [
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in schema_mapping.items()
    ]
    df = loans_repayment_data_df.select(*transformed_columns)
    df = df.withColumn("ingest_date", current_timestamp())
    repay_columns_to_check = [
        "total_principal_received", "total_interest_received",
        "total_late_fee_received", "total_payment_received", "last_payment_amount"
    ]
    df = df.na.drop(subset=repay_columns_to_check)
    # Fix total_payment_received if 0 but subcolumns have data
    df = df.withColumn(
        "total_payment_received",
        when(
            (col("total_principal_received") != 0.0) & (col("total_payment_received") == 0.0),
            col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
        ).otherwise(col("total_payment_received"))
    )
    df = df.filter(col("total_payment_received") != 0.0)
    # Fix last_payment_date and next_payment_date (replace 0.0 with None)
    df = df.withColumn(
        "last_payment_date",
        when(col("last_payment_date") == "0.0", None).otherwise(col("last_payment_date"))
    ).withColumn(
        "next_payment_date",
        when(col("next_payment_date") == "0.0", None).otherwise(col("next_payment_date"))
    )
    return df

# 10. Define the schema mapping for the loans defaulter data and clean it
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
    schema_mapping = get_loans_defaulter_schema_mapping()
    df = loans_defaulter_data_df.select([
        col(old_col).cast(dtype).alias(new_col)
        for old_col, (new_col, dtype) in schema_mapping.items()
    ])
    # Fill nulls in delinq_2yrs with 0 and cast to int
    df = df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("int")).fillna(0, subset=["delinq_2yrs"])
    # Delinquency DataFrame
    delinq_df = df.withColumn("mths_since_last_delinq", col("mths_since_last_delinq").cast("int")) \
        .filter((col("delinq_2yrs") > 0) | (col("mths_since_last_delinq") > 0)) \
        .select("member_id", "delinq_2yrs", "delinq_amnt", "mths_since_last_delinq")
    # Enquiry/Record DataFrame
    detail_df = df.withColumn("pub_rec", col("pub_rec").cast("int")).fillna(0, subset=["pub_rec"]) \
        .withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("int")).fillna(0, subset=["pub_rec_bankruptcies"]) \
        .withColumn("inq_last_6mths", col("inq_last_6mths").cast("int")).fillna(0, subset=["inq_last_6mths"]) \
        .select("member_id", "pub_rec", "pub_rec_bankruptcies", "inq_last_6mths")
    return delinq_df, detail_df

# 11. Create loans verification data table
def create_loans_verification_df(
    customers_data_final, loans_data_final, loans_repayment_final,
    loans_def_delinq_df, loans_def_detail_records_enq_df, spark
):
    df = (
        customers_data_final.alias("c")
        .join(loans_data_final.alias("ldf"), col("c.member_id") == col("ldf.member_id"), "left")
        .join(loans_repayment_final.alias("r"), col("ldf.loan_id") == col("r.loan_id"), "left")
        .join(loans_def_delinq_df.alias("d"), col("c.member_id") == col("d.member_id"), "left")
        .join(loans_def_detail_records_enq_df.alias("e"), col("c.member_id") == col("e.member_id"), "left")
        .select(
            col("ldf.loan_id"), col("c.member_id"), col("c.emp_title"), col("c.emp_length"),
            col("c.home_ownership"), col("c.annual_income"), col("c.address_state"),
            col("c.address_zipcode"), col("c.address_country"), col("c.grade"),
            col("c.sub_grade"), col("c.verification_status"), col("c.total_high_credit_limit"),
            col("c.application_type"), col("c.joint_annual_income"), col("c.verification_status_joint"),
            col("ldf.loan_amount"), col("ldf.funded_amount"), col("ldf.loan_term_years"),
            col("ldf.interest_rate"), col("ldf.monthly_installment"), col("ldf.issue_date"),
            col("ldf.loan_status"), col("ldf.loan_purpose"),
            col("r.total_principal_received"), col("r.total_interest_received"),
            col("r.total_late_fee_received"), col("r.last_payment_date"), col("r.next_payment_date"),
            col("d.delinq_2yrs"), col("d.delinq_amnt"), col("d.mths_since_last_delinq"),
            col("e.pub_rec"), col("e.pub_rec_bankruptcies"), col("e.inq_last_6mths")
        )
    )
    return df

# 12. create bad customers data table
def create_bad_customer_df(customers_data_final, loans_def_delinq_df, loans_def_detail_records_enq_df, spark):
    bad_data_customer_df = (
        customers_data_final.groupBy("member_id").agg(count("*").alias("total"))
        .filter(col("total") > 1).select("member_id")
    )
    bad_data_loans_defaulters_delinq_df = (
        loans_def_delinq_df.groupBy("member_id").agg(count("*").alias("total"))
        .filter(col("total") > 1).select("member_id")
    )
    bad_data_loans_defaulters_detail_rec_enq_df = (
        loans_def_detail_records_enq_df.groupBy("member_id").agg(count("*").alias("total"))
        .filter(col("total") > 1).select("member_id")
    )
    bad_customer_data_df = bad_data_customer_df.union(bad_data_loans_defaulters_delinq_df).union(bad_data_loans_defaulters_detail_rec_enq_df)
    bad_customer_data_final_df = bad_customer_data_df.distinct()
    return bad_customer_data_final_df


# 13. Create final customers data table
def create_customers_df(customers_data_final,bad_customer_data_final_df,spark):
    customers_df = customers_data_final.join(bad_customer_data_final_df, on="member_id", how="left_anti")
    return customers_df

# 14. create loan score

def create_loan_score(
    spark,
    customers_df,
    loans_data_final,
    loans_repayment_final,
    bad_customer_data_final_df,
    loans_def_delinq_df,
    loans_def_detail_records_enq_df    
):
    excellent_pts = int(spark.conf.get("spark.sql.excellent_rated_pts"))
    very_good_pts = int(spark.conf.get("spark.sql.very_good_rated_pts"))
    good_pts = int(spark.conf.get("spark.sql.good_rated_pts"))
    bad_pts = int(spark.conf.get("spark.sql.bad_rated_pts"))
    very_bad_pts = int(spark.conf.get("spark.sql.very_bad_rated_pts"))
    unacceptable_pts = int(spark.conf.get("spark.sql.unacceptable_rated_pts"))

    loans_repayment_scored = loans_repayment_final.alias("p") \
        .join(loans_data_final.select("loan_id", "monthly_installment", "funded_amount").alias("l"),
              col("p.loan_id") == col("l.loan_id")) \
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
        .join(loans_data_final.select("loan_id", "member_id").alias("ld"),
              col("p.loan_id") == col("ld.loan_id"), "inner") \
        .select(col("ld.member_id"), "last_payment_pts", "total_payment_pts")

    fh_ldh_ph_df = loans_def_detail_records_enq_df.alias("ld") \
        .join(loans_def_delinq_df.alias("d"), col("ld.member_id") == col("d.member_id"), "inner") \
        .join(repayment_with_member.alias("p"), col("ld.member_id") == col("p.member_id"), "inner") \
        .join(loans_data_final.alias("loans"), col("ld.member_id") == col("loans.member_id"), "inner") \
        .join(customers_df.alias("a"), col("ld.member_id") == col("a.member_id"), "inner") \
        .join(bad_customer_data_final_df.alias("bad"), col("ld.member_id") == col("bad.member_id"), "left_anti") \
        .select(
            col("ld.member_id"),
            col("p.last_payment_pts"), col("p.total_payment_pts"),
            col("d.delinq_2yrs").cast("int").alias("delinq_pts"),
            col("ld.pub_rec").alias("public_records_pts"),
            col("ld.pub_rec_bankruptcies").alias("public_bankruptcies_pts"),
            col("ld.inq_last_6mths").alias("enq_pts"),
            col("loans.loan_status"), col("a.home_ownership"), col("a.total_high_credit_limit"),
            col("loans.funded_amount"), col("a.grade"), col("a.sub_grade")
        )

    loan_score = fh_ldh_ph_df.select(
        col("member_id"),
        ((col("last_payment_pts") + col("total_payment_pts")) * 0.20).alias("payment_history_pts"),
        ((col("delinq_pts") + col("public_records_pts") + col("public_bankruptcies_pts") + col("enq_pts")) * 0.45).alias("defaulters_history_pts"),
        (
            (when(lower(col("loan_status")).like("%fully paid%"), excellent_pts)
            .when(lower(col("loan_status")).like("%current%"), good_pts)
            .when(lower(col("loan_status")).like("%in grace period%"), bad_pts)
            .when(lower(col("loan_status")).like("%late%"), very_bad_pts)
            .when(lower(col("loan_status")).like("%charged off%"), unacceptable_pts)
            .otherwise(unacceptable_pts))
            +
            (when(lower(col("home_ownership")).like("%own"), excellent_pts)
            .when(lower(col("home_ownership")).like("%rent"), good_pts)
            .when(lower(col("home_ownership")).like("%mortgage"), bad_pts)
            .otherwise(very_bad_pts))
            +
            (when(col("funded_amount") <= col("total_high_credit_limit") * 0.10, excellent_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.20, very_good_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.30, good_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.50, bad_pts)
            .when(col("funded_amount") <= col("total_high_credit_limit") * 0.70, very_bad_pts)
            .otherwise(unacceptable_pts))
            +
            (when(col("grade") == 'A', excellent_pts)
            .when(col("grade") == 'B', very_good_pts)
            .when(col("grade") == 'C', good_pts)
            .when(col("grade") == 'D', bad_pts)
            .when(col("grade") == 'E', very_bad_pts)
            .otherwise(unacceptable_pts))
        * 0.35).alias("financial_health_pts")
    )


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
