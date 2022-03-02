#Darin Hui
#CPSC 4330 Prof. Li
#3/4/22

from pyspark import SparkContext
import sys

#function to add two points (x and y values) together
def add(p1, p2):
    return (p1[0] + p2[0], p1[1] + p2[1])

#returns the distance squared between two points (x values subtracted squared) + (y values subtracted squared)
def squaredDistance(p1, p2):
    return (pow(p1[0] - p2[0], 2) + pow(p1[1] - p2[1], 2))

#returns the index of the closest center from a given point
def closestPoint(point, centerPoints):
    index = 0
    closest = float('inf')
    for i in range(len(centerPoints)):
        tempDist = squaredDistance(point, centerPoints[i])
        if tempDist < closest:
                closest = tempDist
                index = i
    return index

k = 5
convergeDist = 0.1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: hw4.py <input> <output>"
        exit(-1)

    sc = SparkContext()
    textFile = sc.textFile(sys.argv[1])
    
    #Persisted RDD of coordinates that only includes known locations
    locations = textFile.map(lambda line: line.split(",")) \
        .map(lambda values: (float(values[3]), float(values[4]))) \
        .filter((lambda coord: coord[0] != 0.0) and (lambda coord: coord[1] != 0.0)) \
        .persist()

    #Takes a random k points as starting centers
    initialKPoints = locations.takeSample(False, k, 34)
    #temporary comparison distance set to inf (some high number)
    tempDist = float('inf')

    #while loop to maintain iterations, iterate again if centers changed by more than 0.1, terminate if not
    while (tempDist > convergeDist):
        #Finds all points closest to each center
        closest = locations.map(lambda point: (closestPoint(point, initialKPoints), (point, 1)))
        #Sums all latitudes and longitudes together and calculates number of closest points
        centerSum = closest.reduceByKey(lambda pair, pair2: (add(pair[0], pair2[0]), pair[1] + pair2[1]))
        #Finds the new center (mean) for each cluster by taking latitude and longitude sums and dividing by number of closest points
        newPoints = centerSum.map(lambda cluster: (cluster[0], (cluster[1][0], cluster[1][1]))) \
            .map(lambda cluster: (cluster[0], (cluster[1][0][0] / cluster[1][1], cluster[1][0][1] / cluster[1][1]))) \
            .map(lambda center: (center[0], list(center[1]))).collect()
        
        #reset tempDist to calculate the new distances
        tempDist = 0.0
        #calculate the distance between current center and new center 
        #uses corresponding center index in newPoints to ensure consistent clustering
        for i in range(k):
            tempDist += squaredDistance(initialKPoints[newPoints[i][0]], newPoints[i][1])
        #update the new center points by the corresponding center index 
        #in newPoints to ensure consistent clustering
        for (i, point) in newPoints:
            initialKPoints[i] = point

    #convert output to RDD
    result = sc.parallelize(initialKPoints)
    result.repartition(1).saveAsTextFile(sys.argv[2])
    sc.stop()
        
    