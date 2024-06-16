import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("homework_2").getOrCreate()
schema_person = StructType([
    StructField("age", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("birth_date", StringType(), nullable=False),
    StructField("gender", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("salary", DoubleType(), nullable=False)
])
schema_avg_salary = StructType([
    StructField("country", StringType(), nullable=False),
    StructField("avg_salary", DoubleType(), nullable=False)
])
df_person = spark.read \
    .option('header', 'true') \
    .schema(schema_person) \
    .csv('/opt/spark-apps/data/person_sample.csv')
df_avg_salary = spark.read \
    .option('header', 'true') \
    .option('delimiter', '\t') \
    .schema(schema_avg_salary) \
    .csv('/opt/spark-apps/data/avg_salary_data.csv')

# Task 1
df_aggregated = df_person.groupBy("country", "age").agg(
    fn.avg("salary").alias("avg_salary"),
    fn.max("salary").alias("max_salary")
)
df_aggregated.write.csv("/opt/spark-apps/data/aggregated_data.csv", header=True)

# Task 2: Find youngest high earners
df_high_earners = df_person.filter(fn.col("salary") > 40000)
df_min_age = df_high_earners.groupBy("country").agg(fn.min("age").alias("youngest_age"))
df_youngest_high_earners = df_high_earners.join(
    df_min_age,
    (df_high_earners["country"] == df_min_age["country"]) & 
    (df_high_earners["age"] == df_min_age["youngest_age"])
).select(df_high_earners["country"], df_high_earners["name"], df_high_earners["age"], df_high_earners["salary"])
df_youngest_high_earners.show()

# Task 3: Update average salary
df_avg_person_salary = df_person.groupBy("country").agg(
    fn.avg("salary").alias("country_avg_salary"))

df_joined = df_person \
    .join(df_avg_salary, on="country", how="left") \
    .join(df_avg_person_salary, on='country', how='left')

df_updated_salary = df_joined.withColumn(
    "avg_salary",
    fn.when(fn.col("country_avg_salary").isNotNull() & (fn.col("country_avg_salary") > fn.col("avg_salary")),
           fn.col("country_avg_salary")).otherwise(fn.col("avg_salary"))
).drop("avg_salary")
df_updated_salary.write.csv("/opt/spark-apps/data/updated_avg_salary.csv", header=True)

# Task 4: Estimate salary status
df_avg_salary_state = df_person.groupBy("state").agg(fn.avg("salary").alias("state_avg_salary"))

df_joined_state = df_person.join(df_avg_salary_state, on="state", how="inner")

df_salary_status = df_joined_state.withColumn(
    "status",
    fn.when(fn.col("salary") >= fn.col("state_avg_salary"),
           fn.when(fn.col("gender") == "Female", "She is outstanding!").otherwise("He is outstanding!")
          ).otherwise(
           fn.when(fn.col("gender") == "Female", "She is good.").otherwise("He is good.")
          )
)
df_salary_status.write.csv("/opt/spark-apps/data/salary_status.csv", header=True)
