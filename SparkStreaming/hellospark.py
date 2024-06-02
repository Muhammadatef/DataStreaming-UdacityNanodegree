from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
file_path =  "/home/workspace/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')
# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'a' has been encountered (including in this row)
logfile = spark.read.text(file_path).cache()
# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'b' has been encountered (including in this row)
numsA= logfile.filter(logfile.value.contains('a')).count()

numsB= logfile.filter(logfile.value.contains('b')).count()

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
print(f"the number of char a = {numsA}")

# TO-DO: print the count for letter 'a' and letter 'b'
print(f"the number of char a = {numsB}")

# TO-DO: stop the spark application
spark.stop()


## ./spark-submit /home/workspace/hellospark.py
#output:
# 24/06/02 14:06:08 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
#the number of char a = 4
#the number of char b = 0