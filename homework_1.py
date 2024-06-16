import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Homework_1").getOrCreate()
schema_person = StructType([
    StructField("age", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("salary", DoubleType(), True)
])
df_person = spark.read \
    .option('header', 'true') \
    .schema(schema_person) \
    .csv('/opt/spark-apps/data/person_sample.csv')

# Task 1
df_cleaned_person = df_person.withColumn(
    'age',
    fn.when(fn.col('birth_date').isNull() & (fn.col('gender') == 'Male'), 30)
    .when(fn.col('birth_date').isNull() & (fn.col('gender') == 'Female'), 27)
    .when(fn.col('birth_date').isNull() & (fn.col('gender') == 'None'), 10)
    .otherwise(fn.col('age'))
)
df_cleaned_person.filter(fn.col("birth_date").isNull()).write.csv("/opt/spark-apps/data/cleaned_person.csv", header=True)


# Task 2
df_missing_country_or_state = df_person.filter(fn.col("country").isNull() | fn.col("state").isNull())
df_missing_country_or_state.write.csv("/opt/spark-apps/data/missed_country_or_state.csv", header=True)


# Task 3
df_judy = df_person.withColumn("is_judy_over_30", 
    fn.when((fn.col("name") == "Judy") & (fn.col("age") > 30), 1).otherwise(0)
)
count = df_judy.agg(fn.sum("is_judy_over_30")).collect()[0][0]
print(f"Number of Judys over 30: {count}")


spark.stop()
