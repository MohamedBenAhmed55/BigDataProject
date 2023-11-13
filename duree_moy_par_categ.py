from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("AverageDurationByCategory").getOrCreate()
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)
map_result = historique_visualisation.rdd.map(lambda row: (row['Topics'], row['Duration']))
reduce_result = map_result.groupByKey().map(lambda x: (x[0], sum(x[1]) / len(x[1])))
result_df = reduce_result.toDF(["Category", "AverageDuration"])
sorted_result_df = result_df.orderBy(col("Category"))
sorted_result_df.show(truncate=False)
spark.stop()
