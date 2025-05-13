# LendingClubProject
A data pipeline to process Lending Club customers data and generate a loan eligibility score for each customer

Lending Club Data Processing Pipeline
Overview

This project implements an end-to-end data processing pipeline for Lending Club loan data using PySpark. The pipeline ingests raw customer and loan datasets, performs extensive data cleaning and transformation, and generates curated tables for downstream analytics, loan verification, and risk assessment.

The core objective is to structure, clean, and enrich Lending Club data so it can be used for advanced analytics, such as identifying risky loan applicants, understanding default patterns, and supporting machine learning models for credit risk prediction.
Features

    Spark-based scalable ETL: Efficient handling of large Lending Club datasets using distributed processing.

    Data cleaning and schema normalization: Standardizes customer, loan, repayment, and defaulter data with robust null handling and type casting.

    Unique member ID generation: Deterministically generates a unique hash-based ID per customer.

    Loan risk segmentation: Supports scoring and flagging of high-risk (bad) customers.

    Curated output tables: Produces clean, analysis-ready tables for customers, loans, repayments, defaulters, and loan verification.

Directory Structure

    applicationMain.py - Main pipeline entry point; orchestrates the ETL process.

    lib/

        dataManipulation.py - All transformation and cleaning logic for each data entity.

        dataReader.py - Functions for reading raw data into Spark DataFrames.

        utils.py - Utility functions, including Spark session creation and configuration.

Pipeline Steps

    Spark Session Initialization:
    Starts a Spark session based on the specified environment.

    Data Ingestion:
    Reads raw customer data into a Spark DataFrame.

    Configuration:
    Sets rating and grade thresholds as Spark configuration variables.

    Member ID Generation:
    Creates a unique member_id for each customer by hashing key columns.

    View Creation:
    Creates a temporary Spark SQL view for raw customer data.

    DataFrame Construction:

        customers_data_df: Core customer attributes.

        loans_data_df: Loan details.

        loans_repayment_df: Repayment history.

        loans_defaulter_df: Default and delinquency details.

    Data Cleaning:

        Standardizes column names and types.

        Handles nulls and outliers.

        Normalizes categorical fields (e.g., loan purpose, state codes).

        Adds ingestion timestamps.

    Defaulter Data Splitting:
    Separates delinquency info from public records and enquiries.

    Loan Verification Table:
    Joins all curated tables to produce a comprehensive verification dataset per loan.

    Bad Customer Identification:
    Flags customers with default or delinquency records.

    Final Output:

        Cleaned customer, loan, repayment, and defaulter tables.

        Loan verification and risk segmentation tables.

How to Run

bash
spark-submit applicationMain.py <environment>

Replace <environment> with your desired configuration (e.g., dev, prod).
Ensure all dependencies in lib/ are accessible in your Python path.
Dependencies

    Python 3.x

    PySpark

    (Optional) Hadoop/Spark cluster for distributed execution

Data Sources

    Lending Club loan and customer datasets (CSV or Parquet format, schema as per Lending Club documentation)

    .

Customization

    Rating and grade thresholds can be adjusted in applicationMain.py via Spark configuration.

    Schema mappings for each entity are defined in dataManipulation.py and can be modified as needed.

Output

    Curated Spark DataFrames (can be saved as Parquet, CSV, or loaded into a database)

    Console output for schema and sample rows at each major step

Example: Member ID Generation

A unique 32-character member_id is generated for each customer by hashing key identifying columns:

python
id_cols = [
    "emp_title", "emp_length", "home_ownership", "annual_inc",
    "zip_code", "addr_state", "grade", "sub_grade", "verification_status"
]
customers_df.withColumn(
    "member_id",
    sha2(concat_ws("||", *id_cols), 256)
)

Notes

    The pipeline is modular and can be extended for additional data sources or analytics.

    All cleaning steps are transparent and can be audited in dataManipulation.py.

Authors

    Aniruddh Sharma

References

    Lending Club Data Documentation

Kaggle Lending Club Loan Dataset
License

MIT License (or specify your own)

This project is for educational and analytical purposes and is not affiliated with Lending Club.
