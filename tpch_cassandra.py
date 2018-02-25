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

def output_format(kv):
    k, v = kv
    return '%s %s' % (k, v)
def main():
    # do things...
    rdd = sc.cassandraTable(keyspace, 'orders')\
    .select('orderkey','totalprice').where('orderkey in ?',orderkeys)
    lineitem_table=sc.cassandraTable(keyspace, 'lineitem')\
    .select('partkey','orderkey').where('orderkey in ?',orderkeys)
    #Used a .cache() because this rdd is used in many steps
    lineitem_table.cache()
    lineitem=lineitem_table.map(lambda x:x['partkey'])
    part=sc.cassandraTable(keyspace, 'part')\
    .select('partkey','name').where('partkey in ?',lineitem.collect())
    #Used a lineitem.collect() because the partkeys in lineitem rdd are less in number
    res=rdd.map(lambda x:(x['orderkey'],x['totalprice']))
    part=part.map(lambda x: (x['partkey'],x['name']))
    lineitem_table=lineitem_table.map(lambda x: (x['partkey'],x['orderkey']))
    joined=lineitem_table.join(part)
    joined=joined.map(lambda x:x[1])
    final_rdd=joined.join(res).map(lambda x:('Order #'+str(x[0])\
    +' $'+str(float("{0:.2f}".format(x[1][1])))+':',x[1][0]))
    values=final_rdd.reduceByKey(lambda x,y : x+','+y).map(output_format)
    values.saveAsTextFile(outdir)
if __name__ == "__main__":
    main()
