""" !apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
!tar xf spark-3.5.1-bin-hadoop3.tgz
!pip install -q findspark """

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.1-bin-hadoop3"

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, mean, stddev, min, max






from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SnowflakeConnection") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.19,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")\
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

snowflake_options_raw = {
    "sfUrl": os.environ.get("SF_URL>"),
    "sfUser": os.environ.get("SF_USERNAME>"),
    "sfPassword": os.environ.get("SF_PASSWORD>"),
    "sfDatabase": os.environ.get("SF_DATABASE>"),
    "sfSchema": "RAW", 
    "sfWarehouse": "DW_RAW",
}


# Read from Snowflake
snowflake_table = "<your_snowflake_table>"
df = spark.read.format("snowflake").options(**snowflake_options_raw).option("dbtable", snowflake_table).load()


# Create a SparkSession
spark = SparkSession.builder.appName("DataQuality").getOrCreate()

# Read the CSV file into a DataFrame for testing local
df = spark.read.csv("customer_segmentation_data_test.csv", header=True, inferSchema=True)



# Show the first 5 rows
print("First 5 rows:")
df.show(5)

# Print the schema
print("Schema:")
df.printSchema()

# Calculate and print missing values for each column
print("Missing Values:")
for column in df.columns:
    missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
    missing_percentage = (missing_count / df.count()) * 100
    print(f"{column}: {missing_count} ({missing_percentage:.2f}%)")

# Filter numeric columns
numeric_columns = [col_type[0] for col_type in df.dtypes if col_type[1] in ('int', 'bigint', 'float', 'double', 'decimal')]
numeric_df = df.select(numeric_columns)

# Calculate and print descriptive statistics for numeric columns
print("Descriptive Statistics for Numeric Columns:")
for column in numeric_columns:
    stats = numeric_df.select(
        count(column).alias("count"),
        mean(column).alias("mean"),
        stddev(column).alias("stddev"),
        min(column).alias("min"),
        max(column).alias("max")
    ).collect()[0]
    print(f"{column}: {stats}")

# Filter non-numeric columns
non_numeric_columns = [col_type[0] for col_type in df.dtypes if col_type[1] not in ('int', 'bigint', 'float', 'double', 'decimal')]
non_numeric_df = df.select(non_numeric_columns)

# Calculate and print unique value counts for non-numeric columns
print("Unique Value Counts for Non-Numeric Columns:")
for column in non_numeric_columns:
    unique_value_counts = non_numeric_df.groupBy(column).count().collect()
    print(f"{column}:")
    for row in unique_value_counts:
        print(f"  {row[0]}: {row[1]}")