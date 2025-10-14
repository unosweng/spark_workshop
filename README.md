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

## 4 & 5. Count Word Occurrence and Show Top 5

key_value_rdd = words_rdd.map(lambda word: (word, 1))
word_counts_rdd = key_value_rdd.reduceByKey(lambda x, y: x + y)
flipped_rdd = word_counts_rdd.map(lambda item: (item[1], item[0]))
results_rdd = flipped_rdd.sortByKey(False)
results_rdd.take(5)
[(23407, 'the'), (19540, 'I'), (18358, 'and'), (15682, 'to'), (15649, 'of')]
