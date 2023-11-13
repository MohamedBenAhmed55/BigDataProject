from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("TopRecommendedVideos").getOrCreate()
historique_visualisation = spark.read.csv("historique_visualisation.csv", header=True, inferSchema=True)
als = ALS(userCol="UserID", itemCol="VideoID", ratingCol="Duration", coldStartStrategy="drop")
model = als.fit(historique_visualisation)
userRecs = model.recommendForAllUsers(5)  # Vous pouvez ajuster le nombre de recommandations
result_df = userRecs.select("UserID", col("recommendations.VideoID").alias("RecommendedVideos"))
result_df.show(truncate=False)
spark.stop()
