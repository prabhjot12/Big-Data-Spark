from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import json


inputs = sys.argv[1]
output = sys.argv[2]


conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def add_pairs(value1,value2):
    return tuple(map(lambda x, y: x + y, value1, value2))



def get_json(json_obj):
    subreddit_dict=json.loads(json_obj)
    yield (subreddit_dict['subreddit'],(1,subreddit_dict['score']))


def get_key(kv):
    return kv[0]

def get_value(kv):
    return kv[1]


def output_format(kv):
    return json.dumps(kv)

text = sc.textFile(inputs)
subreddits = text.flatMap(get_json)
count = subreddits.reduceByKey(add_pairs)
outdata = count.map(lambda x:(x[0],(x[1][1]/x[1][0])))
final_outdata=outdata.map(output_format)
final_outdata.saveAsTextFile(output)
