
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce,row_number,lit, concat_ws,substring,date_format
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window

# Configuration and Logging

# Enhanced Logging Setup
logging.basicConfig(
    filename='santex_stage.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Table creation mode (options: overwrite, append, ignore, errorIfExists)
mode = "overwrite"

snowflake_options_raw = {
    "sfUrl": os.environ.get("SF_URL>"),
    "sfUser": os.environ.get("SF_USERNAME>"),
    "sfPassword": os.environ.get("SF_PASSWORD>"),
    "sfDatabase": os.environ.get("SF_DATABASE>"),
    "sfSchema": "RAW", 
    "sfWarehouse": "DW_RAW",
}

snowflake_options_stage = {
    "sfUrl": os.environ.get("SF_URL>"),
    "sfUser": os.environ.get("SF_USERNAME>"),
    "sfPassword": os.environ.get("SF_PASSWORD>"),
    "sfDatabase": os.environ.get("SF_DATABASE>"),
    "sfSchema": "DW_PROD", 
    "sfWarehouse": "DW_PROD",
}

# Create SparkSession
spark = SparkSession.builder \
    .appName("ProdProcessing") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.19,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")\
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)


# Data Extraction and Transformation

def load_data_from_snowflake(table_name, options=snowflake_options_raw):
    return spark.read.format("snowflake").options(**options).option("dbtable", table_name).load()

def write_to_snowflake(df, table_name, mode="overwrite", options=snowflake_options_stage):
    (df.write.format("snowflake").options(**options).option("dbtable", table_name).mode(mode).save())

def transform_data(raw_df):
    # Data Cleaning
    df = raw_df.withColumn(
        "Purchase History 2", 
        coalesce(to_date(col("Purchase History"), "MM-dd-yyyy"), to_date(col("Purchase History"), "MM/dd/yyyy"))
    ).drop("Purchase History").withColumnRenamed("Purchase History 2", "Purchase History").withColumnRenamed("Customer ID", "Source_Customer_ID")

    # Generate Customer Key
    window_spec = Window.orderBy(df["Purchase History"], df["Source_Customer_ID"].desc())
    df = df.withColumn("Customer_SK", row_number().over(window_spec))

    write_to_snowflake(df, table_name = 'STG_CUSTOMER_SEGMENTATION_DATA')
    return df

# Create Dimension and Fact Tables

def create_dim_tables(STAGE_CUSTOMER_SEGMENTATION_DATA_DF):

    DIM_CUSTOMER = STAGE_CUSTOMER_SEGMENTATION_DATA_DF[[
        'Customer_SK', 'Source_Customer_ID', 'Age', 'Gender', 'Marital Status',
        'Education Level', 'Geographic Information', 'Occupation', 'Income Level',
        'Behavioral Data', 'Purchase History', 'Interactions with Customer Service',
        'Insurance Products Owned', 'Customer Preferences', 'Preferred Communication Channel',
        'Preferred Contact Time', 'Preferred Language', 'Segmentation Group'
    ]].withColumnRenamed("Purchase History", "Last_Purchase_date")


    DIM_GEOGRAPHY  = (
        STAGE_CUSTOMER_SEGMENTATION_DATA_DF.select("Geographic Information")
        .distinct()
        .withColumn("Country", lit("IN")).withColumnRenamed("Geographic Information", "subdivision_name")
    )
    DIM_GEOGRAPHY = DIM_GEOGRAPHY.withColumn("subdivision_id", concat_ws("-", DIM_GEOGRAPHY.Country, row_number().over(Window.orderBy("subdivision_name"))))
    DIM_GEOGRAPHY = DIM_GEOGRAPHY.select("subdivision_id", "subdivision_name", "Country")

    write_to_snowflake(DIM_GEOGRAPHY, table_name = 'DIM_GEOGRAPHY')

    DIM_POLICY = (
        STAGE_CUSTOMER_SEGMENTATION_DATA_DF
        .select("Policy Type")
        .distinct()
        .withColumn("Policy_ID", substring("Policy Type", 1, 1)).orderBy("Policy_ID")
    )

    write_to_snowflake(DIM_POLICY, table_name = 'DIM_POLICY')

    DIM_INSURANCE_PRODUCTS = (
        STAGE_CUSTOMER_SEGMENTATION_DATA_DF
        .select("Insurance Products Owned")
        .distinct()
        .withColumn("insurance_product_id", substring("Insurance Products Owned", -1, 1))
    ).withColumnRenamed("Insurance Products Owned", "insurance_product_name").select("Insurance_product_id", "insurance_product_name").orderBy("Insurance_product_id")


    write_to_snowflake(DIM_INSURANCE_PRODUCTS, table_name = 'DIM_INSURANCE_PRODUCTS')

    DIM_DATE = (
        STAGE_CUSTOMER_SEGMENTATION_DATA_DF
        .select("Purchase History")
        .distinct()
        .withColumnRenamed("Purchase History", "date")
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast(IntegerType()))
        .orderBy("date_key").select("date_key","date")
    )
    write_to_snowflake(DIM_DATE, table_name = 'DIM_DATE')

    return DIM_DATE, DIM_GEOGRAPHY, DIM_POLICY, DIM_CUSTOMER, DIM_INSURANCE_PRODUCTS

def create_fact_table(stage_data, dim_customer, dim_geography, dim_policy, dim_date):
    fact_df = (
        stage_data.alias("s")
        .join(dim_customer.alias("c"), col("s.Customer_SK") == col("c.Customer_SK"))
        .join(dim_geography.alias("g"), col("s.Geographic Information") == col("g.subdivision_name"))
        .join(dim_policy.alias("p"), col("s.Policy Type") == col("p.Policy Type"))
        .join(dim_date.alias("d"), col("s.Purchase History") == col("d.date"))
        .select(
            col("c.Customer_SK"),
            col("g.subdivision_id").alias("Geographic_Id"),
            col("p.Policy_id"),
            col("d.date_key").alias("Purchase_date"),
            col("s.Insurance Products Owned").alias("Insurance_Product"),  # Consider using Insurance_Product_id here
            col("s.Coverage Amount").alias("Coverage_Amount"),
            col("s.Premium Amount").alias("Premium_Amount")
        )
    )

    write_to_snowflake(FACT_INSURANCE_TRANSACTIONS, table_name = 'FACT_INSURANCE_TRANSACTIONS')

    return fact_df

# Main ETL Process

if __name__ == "__main__":

    logging.info("Starting ETL Stage process...")
    RAW_CUSTOMER_SEGMENTATION_DATA_DF = load_data_from_snowflake("CUSTOMER_SEGMENTATION_DATA")
    STAGE_CUSTOMER_SEGMENTATION_DATA_DF = transform_data(RAW_CUSTOMER_SEGMENTATION_DATA_DF)
    DIM_DATE, DIM_GEOGRAPHY, DIM_POLICY, DIM_CUSTOMER, DIM_INSURANCE_PRODUCTS = create_dim_tables(STAGE_CUSTOMER_SEGMENTATION_DATA_DF)
    FACT_INSURANCE_TRANSACTIONS = create_fact_table(STAGE_CUSTOMER_SEGMENTATION_DATA_DF, DIM_CUSTOMER, DIM_GEOGRAPHY, DIM_POLICY, DIM_DATE)
    logging.info("ETL process Stage completed.")




