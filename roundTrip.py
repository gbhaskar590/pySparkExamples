from pyspark.sql import SparkSession
# from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("roundTrip") \
    .getOrCreate()

roundTripDataDf = spark.read.option("header", "true").csv('travelData.csv')
roundTripDf = roundTripDataDf.withColumn('date', F.to_date(F.col("date"), "MM/dd/yyyy"))
travelWindow = Window.partitionBy("id").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding,
                 Window.unboundedFollowing)
leadDf = roundTripDf.withColumn("source", F.first(F.col('start_loc')).over(travelWindow)) \
    .withColumn("destination", F.last(F.col('end_location')).over(travelWindow)) \
    .select("id", "source", "destination").distinct() \
    .withColumn('tripType', F.when(F.col("source") == F.col("destination"), "Round Trip").otherwise("One Way"))
leadDf.orderBy(F.col('id'), F.col('date')).show()

roundTripDf.show()
