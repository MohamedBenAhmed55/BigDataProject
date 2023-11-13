from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("UserVideosCount").getOrCreate()
donnees_contextuelles = spark.read.csv("donnees_contextuelles.csv", header=True, inferSchema=True)
donnees_demographiques = spark.read.csv("donnees_demographiques.csv", header=True, inferSchema=True)
donnees_videos = spark.read.csv("donnees_videos.csv", header=True, inferSchema=True)
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)

videos_with_topics = historique_visualisation.join(donnees_videos, "VideoID")
joined_data = videos_with_topics.join(donnees_demographiques, "UserID").join(donnees_contextuelles, "UserID")
map_result = joined_data.rdd.map(lambda row: ((row['UserID'], row['Topics']), 1))
reduce_result = map_result.reduceByKey(lambda x, y: x + y)
result_formatted = reduce_result.map(lambda x: (x[0][0], (x[0][1], x[1])))
result_df = result_formatted.toDF(["UserID", "CategoryVideosCount"])
result_df_sorted = result_df.orderBy(col("UserID"))
result_df_sorted.show(truncate=False)
spark.stop()
