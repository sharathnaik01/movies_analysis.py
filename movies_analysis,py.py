from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, explode, split, desc

# Start Spark session
spark = SparkSession.builder \
    .appName("Simple Movies Analysis") \
    .getOrCreate()

# Sample data
data = [
    ("Inception", "Action,Sci-Fi", 8.8, 85.0),
    ("Titanic", "Romance,Drama", 7.8, 90.0),
    ("The Dark Knight", "Action,Crime,Drama", 9.0, 88.0),
    ("The Shawshank Redemption", "Drama", 9.3, 80.0),
    ("Avengers: Endgame", "Action,Adventure,Sci-Fi", 8.4, 95.0),
    ("Joker", "Crime,Drama,Thriller", 8.5, 89.0),
    ("Frozen", "Animation,Family", 7.5, 76.0),
    ("Interstellar", "Adventure,Drama,Sci-Fi", 8.6, 92.0)
]

# Define schema
schema = StructType([
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("vote_average", FloatType(), True),
    StructField("popularity", FloatType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Split and explode genres
df = df.withColumn("genre", explode(split(col("genres"), ",")))

# Top 5 Movies by Rating
print("📽️ Top 5 Movies by Rating:")
df.orderBy(desc("vote_average")).select("title", "vote_average").show(5)

# Top Genres by Average Rating
print("🎬 Top Genres by Average Rating:")
df.groupBy("genre").avg("vote_average").orderBy(desc("avg(vote_average)")).show()

spark.stop()
