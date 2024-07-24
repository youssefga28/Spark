import re
from pyspark import SparkConf, SparkContext
import collections
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("Book")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()
sorted_words=collections.OrderedDict(sorted(wordCounts.items(),key=lambda item: item[1],reverse=True))
i=0
for word, count in sorted_words.items():
    if i == 10 :
        break
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
    i=i+1