cluster_seeds = ['199.60.17.171', '199.60.17.188']
from pyspark import SparkConf
import pyspark_cassandra
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
conf = SparkConf().setAppName('example code') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))\
        .set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.appName('example application').getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
keyspace = sys.argv[1]
outdir = sys.argv[2]
orderkeys = sys.argv[3:]

def df_for(keyspace, table, split_size=None):
    df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    return df
def output_format(kv):
    k, v = kv
    return '%s %s' % (k, v)
def main():
    # do things...
    orders=df_for(keyspace,'orders')
    orders.createOrReplaceTempView('Orders')
    lineitem=df_for(keyspace,'lineitem')
    lineitem.createOrReplaceTempView('Lineitem')
    part=df_for(keyspace,'part')
    part.createOrReplaceTempView('Part')
    Res=spark.sql('''SELECT o.*, p.name FROM
      Orders o
      JOIN Lineitem l ON (o.orderkey = l.orderkey)
      JOIN Part p ON (l.partkey = p.partkey)
    WHERE o.orderkey in {} '''.format(tuple(orderkeys)))
    rdd=Res.rdd
    rdd=rdd.map(lambda x:('Order #'+str(x['orderkey'])+' $'+str(float("{0:.2f}".\
    format(x['totalprice'])))+':',x['name'])).reduceByKey\
    (lambda x,y : x+','+y).map(output_format)
    rdd.saveAsTextFile(outdir)

if __name__ == "__main__":
    main()
