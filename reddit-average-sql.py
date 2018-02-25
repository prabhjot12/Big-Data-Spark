import sys
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('example application').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
inputs = sys.argv[1]
output = sys.argv[2]

def main():
    # do things...
    input_file= inputs
    schemas = StructType([StructField('subreddit', types.StringType(), False),StructField('score', types.IntegerType(), False)])
    comment=spark.read.schema(schemas).json(input_file)
    comment.createOrReplaceTempView('comments')
    averages = spark.sql("""
    SELECT subreddit, Avg(score)
    FROM comments
    GROUP BY subreddit""")
    averages.write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()
