If you are starting from scratch on the login node:
1) interact 
2) cd BigData/Shakespeare 
3) module load spark 
4) pyspark

<!-- Don't have to do the following
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Test_App") 
sc = SparkContext(conf = conf) -->

## 1. Count the number of lines
lines_rdd = sc.textFile("Complete_Shakespeare.txt")
rdd.count()

words_rdd = lines_rdd.flatMap(lambda line: line.split())
In [6]: words_rdd.count()
Out[6]: 904061

### Count the occurrence of each word

## 2. Count the number of words
>>> words_rdd = lines_rdd.flatMap(lambda line: line.split())
>>> words_rdd.count()
904061

## 3. Count unique words
words_rdd.distinct().count()
67779                                                                   

### Show the top 5 most frequent words

key_value_rdd = words_rdd.map(lambda word: (word, 1))
key_value_rdd.take(5)
[('The', 1), ('Project', 1), ('Gutenberg', 1), ('EBook', 1), ('of', 1)]# spark_workshop
