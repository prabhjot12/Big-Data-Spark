import sys
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession, functions, types, Column
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
#assert spark.version >= '2.2' # make sure we have Spark 2.2+
inputs = sys.argv[1]
source= sys.argv[3]
Destination= sys.argv[4]
output = sys.argv[2]

def main(source,Destination):
    # do things...
    nodes=spark.read.text(inputs)
    split_column=F.split(nodes['value'],':')
    nodes=nodes.withColumn('Node',split_column.getItem(0))
    nodes=nodes.withColumn('Destination',split_column.getItem(1)).drop('value')
    nodes=nodes.withColumn('Explode',F.explode(F.split('Destination',' ')))
    nodes.createOrReplaceTempView('nodes')
    edges=spark.sql('''SELECT Node as Source,Explode as Destination from nodes where Explode!= '' ''')
    edges=edges.cache()
    x=[(str(source),'no source node',0)]
    known_paths=spark.createDataFrame(x,['Node','Source','Distance'])
    for i in range(6):
        new_df=(known_paths.join(edges,edges.Source==known_paths.Node)\
                .select(edges.Destination.alias('Node'),edges.Source,(known_paths.Distance)+1))
        known_paths=known_paths.unionAll(new_df).orderBy('Distance').dropDuplicates(['Node'])
        value_check=known_paths.select('Node').where((known_paths.Node==str(Destination))\
                                                     &(known_paths.Node==str(source))).count()
        known_paths=known_paths.cache()
        known_paths.write.csv(output + '/iter-' + str(i))
        if(value_check==2):
            break
    paths=[]
    for i in range(6):
        value = known_paths.filter(known_paths.Node == str(Destination)).select('Source')
        #If there is no path then the following
        if (value.count() == 0):
            paths.append('No Path Found')
            break
        value = value.head()[0]
        paths.append(str(Destination))
        Destination = value
        if ((Destination == 'no source node') | (Destination == str(source))):
            if (Destination == str(source)):
                paths.append(Destination)
            break
    finalpath = sc.parallelize(paths[::-1])
    finalpath.saveAsTextFile(output + '/path')


if __name__ == "__main__":
    main(source, Destination)
