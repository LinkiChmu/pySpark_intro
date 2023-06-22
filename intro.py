from pyspark import SparkConf
from pyspark import SparkContext

"""The movies.csv file contains the database of movies. 
The title of the movie is written in the second column 'title'.
Break movie titles into individual words and count which word is most common.
"""

conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")
data = sc.textFile('movies.csv')
header = data.first()
result = (
    data.filter(lambda x: x != header)
        .flatMap(lambda x: x.split(',')[1].split(' ')[:-1])
        .map(lambda x: (x, 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(1, key=lambda x: -x[1])
    )[0][0]

print(f'The most common word: {result}')
