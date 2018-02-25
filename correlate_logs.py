cluster_seeds = ['199.60.17.171', '199.60.17.188']
from pyspark import SparkConf
import pyspark_cassandra
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
conf = SparkConf().setAppName('example code') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.appName('example application').getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
keyspace = sys.argv[1]
table = sys.argv[2]

def df_for(keyspace, table, split_size=None):
    df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    return df
def main():
    # do things...
    dFrame=df_for(keyspace,table)
    dFrame.createOrReplaceTempView(table)
    tableData=spark.sql('''select host,count(host) as 
    x,sum(bytes) as y from '''+table+''' group by host ''')
    tableData.createOrReplaceTempView('intermed')
    #tableData has been cached because it is being used twice,
    #once for calculating numerator and once for denominator
    tableData.cache()
    result=spark.sql('''select ((count(*)*sum(x*y))-(sum(x)*sum(y)))
    as numerator from intermed''')
    denominator=spark.sql('''select(sqrt((count(*)*sum(power(x,2)))
    -power(sum(x),2))*sqrt((count(*)*sum(power(y,2)))-
    power(sum(y),2))) as denominator from intermed''')
    res=result.head()[0]/denominator.head()[0]
    print('r: '+ str(res))
    print('r^2: '+str(res**2))
if __name__ == "__main__":
    main()
