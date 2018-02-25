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
keyspace1 = sys.argv[1]
keyspace2 = sys.argv[2]

def df_for(keyspace, table, split_size=None):
    df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    return df

def main():
    # do things...
    orders=df_for(keyspace1,'orders')
    orders.createOrReplaceTempView('Orders')
    lineitem=df_for(keyspace1,'lineitem')
    lineitem.createOrReplaceTempView('Lineitem')
    part=df_for(keyspace1,'part')
    part.createOrReplaceTempView('Part')
    Res=spark.sql('''SELECT o.*, p.name FROM
      Orders o
      JOIN Lineitem l ON (o.orderkey = l.orderkey)
      JOIN Part p ON (l.partkey = p.partkey)''')
    df=Res.groupby('orderkey').agg(F.collect_list('name'))\
    .withColumnRenamed('collect_list(name)','part_names')
    df=df.join(Res,'orderkey').drop('name')
    df.rdd.map(lambda r: r.asDict()).saveToCassandra(keyspace2, 'orders_parts')
if __name__ == "__main__":
    main()
