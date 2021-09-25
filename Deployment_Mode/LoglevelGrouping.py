import sys
from pyspark import SparkContext

def main(logFile):

	#Create SparkContext object
    sc = SparkContext()
    
    #Read the file from cluster
    rdd1 = sc.textFile(logFile)
    
    #Find the Loglevel Grouping
    rdd2 = rdd1.map(lambda x: (x.split(":")[0], x.split(":")[1])).groupByKey().map(lambda x: (x[0], len(x[1])))
    
    #Perform action to print the number of entries
    rdd3 = rdd2.collect()
    
    #Display the results
    for k, v in rdd3:
        print('Key:{k}, Value:{v}'.format(k=k, v=v))
    
    
if __name__ == "__main__":
    args = sys.argv[1]
    #print(args)
    main(args)