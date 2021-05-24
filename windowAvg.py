from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("windowAvg").getOrCreate()
empDf = spark.read.option("header", "true").csv('empData.csv')
deptWindow = Window.partitionBy("deptName").orderBy("salary").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
moreThanAvgSal = empDf.withColumn('avgSalary', F.avg(F.col('salary')).over(deptWindow)) \
.filter(F.col('avgSalary') < F.col('salary'))

moreThanAvgSal.show()