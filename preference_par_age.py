from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FavoriteCategoriesByAge").getOrCreate()
donnees_demographiques = spark.read.csv("donnees_demographiques.csv", header=True, inferSchema=True)
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)
map_result = historique_visualisation.join(donnees_demographiques, "UserID") \
    .rdd.map(lambda row: ((row['Age'], row['Topics']), 1))
reduce_result = map_result.reduceByKey(lambda x, y: x + y)
result_df = reduce_result.toDF(["AgeCategory", "VideoCount"])
sorted_result_df = result_df.orderBy(col("AgeCategory"))
sorted_result_df.show(truncate=False)
spark.stop()
