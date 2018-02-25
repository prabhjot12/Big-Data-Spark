import sys
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as F
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
topic = sys.argv[1]
def get_x(column):
    splits=column.split(' ')
    return float(splits[0])
def get_y(column):
    splits=column.split(' ')
    return float(splits[1])

def main():
    # do things...
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    line=messages.selectExpr('CAST (value as string)')
    my_udf_x=F.UserDefinedFunction(get_x, returnType=types.FloatType())
    my_udf_y=F.UserDefinedFunction(get_y, returnType=types.FloatType())
    line=line.withColumn('X',my_udf_x('value'))
    line=line.withColumn('Y',my_udf_y('value'))
    line=line.drop('value')
    line.createOrReplaceTempView('line')
    df=spark.sql('''select (sum(X*Y)-((1/count(*))*sum(X)*sum(Y)))
    /(sum(power(X,2))-((1/count(*))*power(sum(X),2)))
    as beta,(sum(Y)/count(*)) as lhs,((sum(X))/count(*)) as rhs from line
    ''')
    df.createOrReplaceTempView('final')
    df_final=spark.sql('''select beta,(lhs-(beta*rhs)) as alpha from final''')
    stream= df_final \
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .start()
    stream.awaitTermination(600)
if __name__ == "__main__":
    main()
