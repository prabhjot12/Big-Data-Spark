cluster_seeds = ['199.60.17.171', '199.60.17.188']
from pyspark import SparkConf
import pyspark_cassandra
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
conf = SparkConf().setAppName('example code') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.appName('example application').getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
keyspace = sys.argv[1]
outdir = sys.argv[2]
orderkeys = sys.argv[3:]

def output_format(kv):
    k, v = kv
    return '%s %s' % (k, v)
def to_string(listval):
    name=''
    for i in listval:
        name=name+i+','
    return name[:-1]
def main():
    # do things...
    order = sc.cassandraTable(keyspace, 'orders_parts')\
    .select('orderkey','totalprice','part_names').where('orderkey in ?',orderkeys)
    order=order.map(lambda x:('Order #'+str(x['orderkey'])\
    +' $'+str(float("{0:.2f}".format(x['totalprice'])))+':',\
    to_string(x['part_names']))).map(output_format)
    order.saveAsTextFile(outdir)

if __name__ == "__main__":
    main()
