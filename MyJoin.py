    
# -*- coding: utf-8 -*-
import re
import pyspark
import sys
import os
import pandas as pd

SPARK_HOME = '/root/spark/'
os.environ['SPARK_HOME'] = os.path.join(SPARK_HOME)
sys.path.append('/root/spark/python/')

sc = pyspark.SparkContext()

table1 = sc.textFile('s3n://bigdives3/DataClean/dbo.shop.header.droppedhard.csv').cache()
table2 = sc.textFile('s3n://bigdives3/DataClean/DataClean/dbo.shop.STAT_storico_dett.droppedhard.csv').cache

#table1.take(10).saveAsTextFile('s3n://bigdives3/DataClean/Join_query')
#print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ecce !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
#data = sc.parallelize([1,2,3,4,5,6])
#print data
#print table1.take(10)
print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
print table2.first()

'''
def ExtractHeader(_table):
    temp = _table.first()
    table = _table.filter(lambda x:x !=temp).cache()
    header = temp.split(',')
    print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    print header 
    return table, header

table1, header1 = ExtractHeader(table1)
table2, header2 = ExtractHeader(table2)

def SanityCheck(table, header):
    data_extract = table.map(lambda line: (line.split(','))) \
    .filter(lambda line: len(line) == len(header))
    #print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    #print 'Columns number: ', data_extract.count()
    return data_extract

#table1 = SanityCheck(table1, header1)
#table2 = SanityCheck(table2, header2)

data_extract = table2.map(lambda line: line[4]+line[5]+line[6])
data_extract = table2.map(lambda line: ''.join(re.findall('\d+', line[4]+line[5]+line[6] )))

#aaa = data_extract.filter(lambda line: line == '19782')
frequencies = data_extract.map(lambda w: (w, 1)).reduceByKey(lambda v1,v2: v1+v2)
print frequencies.take(10)
'''

#frequencies.saveAsTextFile('s3n://bigdives3/DataClean/Join_query')
