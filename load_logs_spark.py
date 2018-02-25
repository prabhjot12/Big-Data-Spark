cluster_seeds = ['199.60.17.171', '199.60.17.188']
from pyspark import SparkConf
import pyspark_cassandra
import uuid
import sys
import re
import datetime
conf = SparkConf().setAppName('example code') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
# spark = SparkSession.builder.getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
inputs = sys.argv[1]
keyspace = sys.argv[2]
table = sys.argv[3]

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')

def extract(log):
    each_split=line_re.split(log)
    if(len(each_split)==6):
        yield(each_split[1],each_split[2],each_split[3],int(each_split[4]))

def main():
    # do things...
    text = sc.textFile(inputs)
    splits=text.flatMap(extract)
    splits=splits.repartition(50)
    rows=splits.map(lambda x : ({'host':x[0],'datetime':\
    datetime.datetime.strptime(x[1], '%d/%b/%Y:%H:%M:%S')\
    ,'path':x[2],'bytes':x[3],'id':str(uuid.uuid4())}))
    rows.saveToCassandra(keyspace,table)

if __name__ == "__main__":
    main()
