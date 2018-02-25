import sys
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.appName('example application').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
inputs = sys.argv[1]
output = sys.argv[2]

def path_to_hour(column):
    base=os.path.basename(column)
    file_split=os.path.splitext(base)[0]
    splits=file_split.split('-')
    return splits[1]+'-'+splits[2][0:2]

def main():
    # do things...
    input_file= inputs
    schemas = StructType([StructField('Language', types.StringType(), False),StructField('Page', types.StringType(), False)\
    ,StructField('Requests', types.IntegerType(), False)\
                          ,StructField('Size', types.IntegerType(), False)])
    comment=spark.read.csv(input_file,sep=" ",schema=schemas)
    comment=comment.withColumn('Filename',functions.input_file_name())
    my_udf=F.UserDefinedFunction(path_to_hour, returnType=types.StringType())
    comment=comment.withColumn('Split',my_udf('Filename'))
    comment=comment.drop('Filename').withColumnRenamed('Split','Filename')
    comment=comment.filter((comment.Language=='en')&(comment.Page!='Main_page')&(~(comment.Page.like('Special:%'))))
    max_df=comment.groupBy('Filename').agg({'Requests':'max'})
    max_df=max_df.withColumnRenamed('max(Requests)','Requests')
    final_df=max_df.join(comment,['Filename','Requests'])
    final_df=final_df.select('Filename','Page','Requests').orderBy('Filename')
    final_df.write.csv(output,sep=',')

if __name__ == "__main__":
    main()
