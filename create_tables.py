from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Start Spark
spark = SparkSession.builder.config("spark.driver.host", "localhost") \
    .appName("JobPostingsAnalysis") \
    .getOrCreate()

spark.catalog.clearCache()

# Load data
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv("./data/lightcast_job_postings.csv")

# Register original dataset as SQL table
df.createOrReplaceTempView("job_postings")

# -----------------------------
# CREATE INDUSTRIES TABLE
# -----------------------------
industries_df = df.select(
    col("naics_2022_6"),
    col("naics_2022_6_name"),
    col("soc_5").alias("soc_code"),
    col("soc_5_name").alias("soc_name"),
    col("lot_specialized_occupation_name").alias("specialized_occupation"),
    col("lot_occupation_group").alias("occupation_group")
).distinct().withColumn("industry_id", monotonically_increasing_id())

# Rearrange columns
industries_df = industries_df.select(
    "industry_id",
    "naics_2022_6",
    "naics_2022_6_name",
    "soc_code",
    "soc_name",
    "specialized_occupation",
    "occupation_group"
)

# Show industries table
industries_df.show(5, truncate=False)

# Register industries table in Spark SQL
industries_df.createOrReplaceTempView("industries")

# Optional: save industries table as CSV
industries_df.write.mode("overwrite").option("header", True).csv("./output/industries.csv")

# -----------------------------
# SQL QUERY: TOP 5 MOST POSTED JOB TITLES
# -----------------------------
top_titles = spark.sql("""
    SELECT TITLE_RAW, COUNT(*) AS job_count
    FROM job_postings
    GROUP BY TITLE_RAW
    ORDER BY job_count DESC
    LIMIT 5
""")

# Show SQL result
top_titles.show()

# Optional: convert to pandas for display
top_titles_pd = top_titles.toPandas()
print(top_titles_pd)

# Optional: visualization
import matplotlib.pyplot as plt

top_titles_pd.plot(kind="bar", x="TITLE_RAW", y="job_count", legend=False)
plt.title("Top 5 Most Posted Job Titles")
plt.xlabel("Job Title")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig("./output/top_5_job_titles.png")
plt.close()