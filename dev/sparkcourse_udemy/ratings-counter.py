import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

#spark = pyspark.sql.SparkSession.builder.master("local").appName("spark").getOrCreate()

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcourse_udemy/resources/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
