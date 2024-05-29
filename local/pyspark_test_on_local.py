from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("SparkTest") \
    .getOrCreate()

# Create DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame
df.display()

# Stop SparkSession
