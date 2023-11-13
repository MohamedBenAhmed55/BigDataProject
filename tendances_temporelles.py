from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month

spark = SparkSession.builder.appName("TemporalTrends").getOrCreate()
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)
map_result = historique_visualisation.withColumn("Month", month("Timestamp")).rdd.map(lambda row: (row['Month'], 1))
reduce_result = map_result.reduceByKey(lambda x, y: x + y)
result_df = reduce_result.toDF(["Month", "VideoCount"])
sorted_result_df = result_df.orderBy(col("Month"))
sorted_result_df.show(truncate=False)
spark.stop()
