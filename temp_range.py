import sys
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('Temperature application').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
#assert spark.version >= '2.2' # make sure we have Spark 2.2+
inputs = sys.argv[1]
output = sys.argv[2]

def main():
    # do things...
    input_file= inputs
    schemas = StructType([StructField('Station', types.StringType(), False),StructField('Date', types.IntegerType(), False)\
                          ,StructField('Observation', types.StringType(), False),StructField('Value', types.IntegerType(), False)\
                          ,StructField('MFlag', types.StringType(), False),StructField('QFlag', types.StringType(), False)])
    comment=spark.read.csv(input_file,schema=schemas)
    comment=comment.filter(comment.QFlag.isNull())
    Tmin=comment.filter(comment.Observation=='TMIN')
    Tmin=Tmin.withColumnRenamed('Value','Min')
    Tmax=comment.filter(comment.Observation=='TMAX')
    Tmax=Tmax.withColumnRenamed('Value','Max')
    data=Tmin.join(Tmax,['Station','Date'],'inner')
    df_with_range=data.withColumn('Range',F.lit(data.Max-data.Min))
    df_with_max=df_with_range.groupBy('Date').agg(F.max('Range'))
    df_with_max=df_with_max.withColumnRenamed('max(Range)','Range')
    df_final=df_with_max.join(df_with_range,['Date','Range'],'inner')\
    .orderBy('Date').drop('Observation','Min','Max')
    df_final=df_final.select('Date','Station','Range')
    df_final.write.csv(output,sep=' ')

if __name__ == "__main__":
    main()
