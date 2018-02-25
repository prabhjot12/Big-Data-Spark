import re, string
import os
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import sys
import gzip
import uuid
import datetime
input_dir = sys.argv[1]
keyspace=sys.argv[2]
table_name=sys.argv[3]
cluster = Cluster(['199.60.17.171', '199.60.17.188'])
session = cluster.connect(keyspace)
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
for f in os.listdir(input_dir):
    with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8',\
 errors='ignore') as logfile:
        batch = BatchStatement()
        insert_log = session.prepare('INSERT INTO '+table_name+\
' (host, datetime, path, bytes,id) VALUES (?,?,?,?,?)')
        temp=0
        for line in logfile:
            split=line_re.split(line)
            if (len(split) == 6):
                temp+=1
                batch.add(insert_log,(split[1],datetime.datetime.strptime(split[2], '%d/%b/%Y:%H:%M:%S')\
,split[3],int(split[4]),uuid.uuid4()))
                if(temp==300):
                    session.execute(batch)
                    batch = BatchStatement()
                    temp=0
