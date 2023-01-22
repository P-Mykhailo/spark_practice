import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcourse_udemy/resources/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
totalsByAge1 = totalsByAge.collect()
averagesByAge = totalsByAge.mapValues(lambda x: int(x[0] / x[1]))
results = averagesByAge.sortByKey().collect()
for result in results:
    print(result)
