from pyspark.sql import SparkSession
# from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("roundTrip") \
    .getOrCreate()

roundTripDataDf = spark.read.option("header", "true").csv('travelData.csv')
roundTripDf = roundTripDataDf.withColumn('date', F.to_date(F.col("date"), "MM/dd/yyyy"))
roundTripDf.show()

travelWindow = Window.partitionBy("id").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding,
                 Window.unboundedFollowing)
tripTypeDF = roundTripDf.withColumn("source", F.first(F.col('start_loc')).over(travelWindow)) \
    .withColumn("destination", F.last(F.col('end_location')).over(travelWindow)) \
    .select("id","source","destination").distinct()\
    .withColumn('tripType', F.when(F.col("source") == F.col("destination"), "Round Trip").otherwise("One Way"))
tripTypeDF.orderBy(F.col('id'), F.col('date')).show()

travelWindow2 = Window.partitionBy("id").orderBy("date")
tripMapDf = roundTripDf.withColumn("lead", F.lead(F.col("start_loc")).over(travelWindow2)) \
    .withColumn("lag", F.lag(F.col("end_location")).over(travelWindow2))
tripMapDf.orderBy(F.col('id'), F.col('date')).show()

tripJoinDf = tripTypeDF.join(tripMapDf, on="id", how="inner") \
    .withColumn("t",
                F.when((F.col("lead").isNull()), F.when(F.col("source") == F.col("end_location"), 0).otherwise(1)) \
                .otherwise(F.when(F.col("end_location") == F.col("lead"), 0).otherwise(1)))\
    .groupBy("id").sum("t")\
    .withColumn("trip", F.when(F.col("sum(t)")>0,"One Way").otherwise("Round trip"))\
    .select("id","trip")
tripJoinDf.orderBy(F.col('id')).show()