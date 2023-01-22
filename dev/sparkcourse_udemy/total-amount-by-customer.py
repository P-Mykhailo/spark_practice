import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)

lines = sc.textFile("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcourse_udemy/resources/customer-orders.csv")
rdd = lines.map(parseLine)
customers_amount = rdd.reduceByKey(lambda x, y: (x + y)).map(lambda x: (x[1], x[0])).sortByKey()
results = customers_amount.collect();

for result in results:
    print(int(result[0]), " ", int(result[1]))
