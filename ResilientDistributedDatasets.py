from pyspark.context import SparkContext

# make a list of numbers to be used as input data
numbers = list(range(15))
print(numbers)

# create a spark context and an RDD
sc = SparkContext()
rdd = sc.parallelize(numbers)

# these are transformations, which are lazy
multiplied = rdd.map(lambda x : x*2)
threes = rdd.filter(lambda x : x % 3 == 0)

# this is an action, which causes the transformations above to happen
threes.count()

# this gets the data from the RDD and prints
result = threes.collect()
print(result)

sc.stop()