from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.driver.host", "localhost") \
    .appName("JobPostingsAnalysis") \
    .getOrCreate()

spark.catalog.clearCache()

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv("./data/lightcast_job_postings.csv")

df.createOrReplaceTempView("job_postings")

print("=== CHECK ===")
df.printSchema()
df.show(5)