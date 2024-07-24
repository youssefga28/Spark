from pyspark import SparkConf, SparkContext
import collections
conf = SparkConf().setMaster("local").setAppName("TotalSpentCustomer")
sc = SparkContext(conf = conf)
def parseLine(line):
    line=line.split(',')
    id=int(line[0])
    spent=float(line[2])
    return (id, spent)
orders1=sc.textFile('customer-orders.csv')
orders2=orders1.map(parseLine).reduceByKey(lambda x,y: x+y ).map(lambda x:(x[1],x[0])).sortByKey()
results=orders2.collect()
for result in results:
    print(str(result[1])+': '+str(result[0]))


