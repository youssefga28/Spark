from pyspark import SparkConf, SparkContext
import collections
conf=SparkConf().setMaster("local").setAppName("ratingsHistogram")
sc=SparkContext(conf=conf)

lines=sc.textFile("ml-100k/u.data")
ratings=lines.map(lambda x: x.split() [2])
result=ratings.countByValue()
SortedResults=collections.OrderedDict(sorted(result.items()))
for key, value in SortedResults.items():
    print (key," ",value)