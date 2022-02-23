#Darin Hui
#CPSC 4330 Prof. Li
#2/22/22

from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: hw3.py <input> <output>"
        exit(-1)
    sc = SparkContext()
    textFile = sc.textFile(sys.argv[1])

    #Takes the header from the input file
    fileHeader = textFile.first()

    #Turns header into an RDD
    header = sc.parallelize([fileHeader])

    #Removes header from input data
    data = textFile.subtract(header)

    #Splits the line by tab, maps the product_id and ratings, groups the product_id
    #to return an iterable of ratings, sorts by product_id ascending, and maps
    #the product_id, the average ratings, and the number of ratings given
    ratings = data.map(lambda line: line.split("\t")) \
        .map(lambda values: (values[3], int(values[7]))) \
        .groupByKey() \
        .sortByKey(True) \
        .map(lambda x: (x[0], float(sum(x[1])/len(x[1])), len(x[1])))
        
    ratings.saveAsTextFile(sys.argv[2])
    sc.stop()