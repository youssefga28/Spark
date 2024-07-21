from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(Line):
    fields=Line.split(',')
    friends=int(fields[3])
    age=int(fields[2])
    return (age,friends)
lines=sc.textFile("fakefriends.csv")
rdd=lines.map(parseLine)
totalsByAge=rdd.mapValues(lambda x : (x,1)).reduceByKey( lambda x , y : (x[0]+y[0],x[1]+y[1]))
averagesByAge=totalsByAge.mapValues(lambda x : x[0]/x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
