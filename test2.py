from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

# Create tables
products = spark.createDataFrame([
    (1, "Product1"),
    (2, "Product2"),
    (3, "Product3"),
    (4, "Product7"),
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (0, "Category1"),
    (1, "Category2"),
    (2, "Category3"),
    (3, "Category4"),
    (4, "NullCategory"),
], ["id_category", "category_name"])

links = spark.createDataFrame([
    (1, 0),
    (1, 2),
    (2, 1),
    (4, 2),
    (3, 4),
], ["product_id", "id_category"])

# join
result = links.join(products, on='product_id').join(categories, on="id_category", how="left")
result = result.select("product_name", "category_name")
result.show()
spark.stop()
