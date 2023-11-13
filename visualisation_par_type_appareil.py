from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("DeviceConnectionAnalysis").getOrCreate()

donnees_contextuelles = spark.read.csv("donnees_contextuelles.csv", header=True, inferSchema=True)
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)

map_result = historique_visualisation.join(donnees_contextuelles, "UserID") \
    .rdd.map(lambda row: ((row['Device'], row['ConnectionQuality']), 1))
reduce_result = map_result.reduceByKey(lambda x, y: x + y)
result_df = reduce_result.toDF(["DeviceConnection", "VideoCount"])
sorted_result_df = result_df.orderBy(col("DeviceConnection"))
sorted_result_df.show(truncate=False)
spark.stop()
