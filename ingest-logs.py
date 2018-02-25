import sys
import re, string
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,Row
from pyspark.sql import SparkSession, functions, types
import datetime


spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
#assert spark.version >= '2.2' # make sure we have Spark 2.2+
inputs = sys.argv[1]
output = sys.argv[2]

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')

def extract(log):
    each_split=line_re.split(log)
    if(len(each_split)==6):
        yield(each_split[1],each_split[2],each_split[3],int(each_split[4]))

def main():
    # do things...
    text = sc.textFile(inputs)
    splits=text.flatMap(extract)
    rows=splits.map(lambda x : Row(Host=x[0],DateTime=datetime.datetime.strptime(x[1], '%d/%b/%Y:%H:%M:%S'),Path=x[2],Bytes=x[3]))
    df=spark.createDataFrame(rows)
    df.write.format('parquet').save(output)
    log = spark.read.parquet(output)
    log.createOrReplaceTempView('logs')
    sums = spark.sql("""
    SELECT SUM(Bytes)
    as SUM 
    FROM logs""")
    sums.show()

if __name__ == "__main__":
    main()
