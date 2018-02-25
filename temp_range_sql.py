import sys
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('example application').getOrCreate()
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
    comment.createOrReplaceTempView('comments')
    TMAX=spark.sql("""select * from comments where Observation='TMAX' and QFlag is null""")
    TMIN=spark.sql("""select * from comments where Observation='TMIN' and QFlag is null""")
    TMAX.createOrReplaceTempView('TMAX')
    TMIN.createOrReplaceTempView('TMIN')
    join_1=spark.sql("""select TMIN.Station,TMIN.Date,TMAX.Value-TMIN.Value as Range from TMAX
    inner join TMIN on TMIN.Station=TMAX.Station and TMIN.Date=TMAX.Date""")
    join_1.createOrReplaceTempView('Join1')
    max_range=spark.sql("""select Date,MAX(Range) as Range from Join1 group by Date""")
    max_range.createOrReplaceTempView('MaxRange')
    final=spark.sql("""select MaxRange.Date,Station,MaxRange.Range from MaxRange inner join
    Join1 on MaxRange.Range=Join1.Range and MaxRange.Date=Join1.Date order by Date""")
    final.write.csv(output,sep=' ')


if __name__ == "__main__":
    main()
