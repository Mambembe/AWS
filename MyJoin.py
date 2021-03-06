    
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

#table1 = sc.textFile('s3n://bigdives3/DataClean/dbo.shop.header.droppedhard.csv')
table2 = sc.textFile('s3n://bigdives3/DataClean/dbo.shop.STAT_storico_dett.droppedhard.csv')

def ExtractHeader(table):
    temp = table.first()
    table = table.filter(lambda x:x !=temp)
    header = temp.split(',')
    return table, header


#table1, header1 = ExtractHeader(table1)
table2, header2 = ExtractHeader(table2)

def SanityCheck(table, header):
    data_extract = table.map(lambda line: (line.split(','))) \
    .filter(lambda line: len(line) == len(header))
    return data_extract

#table1 = SanityCheck(table1, header1)
table2 = SanityCheck(table2, header2)

#print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
#print table2.take(6)



#data_extract = table2.map(lambda line: line[4]+line[5]+line[6])
data_extract = table2.map(lambda line: ''.join(re.findall('\d+', line[4]+line[5]+line[6] )))
                    #.map(lambda line: line[0:5])
aaa = data_extract.filter(lambda line: line == '33949')
#aaa = data_extract.filter(lambda line: line == '19782')
#frequencies = data_extract.map(lambda w: (w, 1)).reduceByKey(lambda v1,v2: v1+v2)
#print frequencies.take(10)

#data_extract = table1.map(lambda line: line[2])
aaa.saveAsTextFile('s3n://bigdives3/DataClean/Join_query2')
