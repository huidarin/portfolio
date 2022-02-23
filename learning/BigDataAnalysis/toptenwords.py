#Darin Hui
#2/22/22

from pyspark import SparkContext
import re
import sys

#Function using regular expressions to remove leading/trailing spaces
#and punctation (except apostrophes) from lines of text.
def clean(text):
    lowercase = text.lower()
    punc = re.sub('[^a-zA-Z0-9\s\']', "", lowercase)
    valid = re.sub('\t', " ", punc)
    clean = valid.strip()
    return clean

#Function using regular expressions to remove apostrophes from words after removing 
#stop words, where some contain an apostrophe in it
def clean2(text):
    valid = re.sub('\'', "", text)
    return valid

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: hw3_2.py <input> <output>"
        exit(-1)
    sc = SparkContext()
    textFile = sc.textFile(sys.argv[1])
    stopFile = sc.textFile('/hw/stopwords.txt')

    #Creates a list of the stop words in the .txt file
    stopWords = stopFile.collect()

    #Cleans each line of text to disregard capitalizations and 
    #remove punctuation, except apostrophes, and leading/trailing spaces.
    #Splits each word in a line by space delimiter
    #Filters empty and stop words from the RDD of stop words
    #Cleans each word to remove any apostrophes that weren't filtered by stop words
    #Maps each word and the value 1, reduces by key to aggregate word counts
    #Sort by word counts descending
    words = textFile.map(lambda line: clean(line)) \
        .flatMap(lambda line: line.split(' ')) \
        .filter(lambda word: len(word) > 0) \
        .filter(lambda word: word not in stopWords) \
        .map(lambda word: clean2(word)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x,y: x + y) \
        .map(lambda values: (values[1], values[0])) \
        .sortByKey(False) \
        .map(lambda values: (values[1], values[0]))

    #Takes a list of the top ten highest counting words in the input files
    topTen = words.take(10)

    #Turns the top ten list into an RDD
    result = sc.parallelize(topTen)
    
    #Saves RDD as a text file
    result.repartition(1).saveAsTextFile(sys.argv[2])
    sc.stop()
    
