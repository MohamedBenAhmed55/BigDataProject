from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("TopCategories").getOrCreate()
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)
map_result = historique_visualisation.rdd.map(lambda row: (row['Topics'], 1))
reduce_result = map_result.reduceByKey(lambda x, y: x + y)
result_df = reduce_result.toDF(["Category", "VideoCount"])
sorted_result_df = result_df.orderBy(col("VideoCount").desc())
sorted_result_df.show(truncate=False)
spark.stop()
