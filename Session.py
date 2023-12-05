from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# I had trouble getting this to work because my Java home was not set
# I set it using 'export JAVA_HOME=`/usr/libexec/java_home -v 21.0.1`'
