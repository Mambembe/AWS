# -*- coding: utf-8 -*-

'''
    - Read csv file
    - Delete useless columns
    - Saves the result in a folder with different test files
    - Returns a txt file with some results from the deleting phase
'''

import sys
import os
import pandas as pd
SPARK_HOME = '/root/spark/'
os.environ['SPARK_HOME'] = os.path.join(SPARK_HOME)
sys.path.append('/root/spark/python/')
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row


# --------------------------------------------------------------------
tableName1 = 'dbo.shop.header.droppedhard'
tableName2 = 'dbo.shop.STAT_storico_dett.droppedhard'
# --------------------------------------------------------------------

sc = pyspark.SparkContext()
path1 = 's3n://bigdives3/DataClean/' + tableName1 + '.csv'
path2 = 's3n://bigdives3/DataClean/' + tableName2 + '.csv'

table1 = sc.textFile(path1)
table2 = sc.textFile(path2)

# Divides Header from Table
temp1 = table1.first()
table1 = table1.filter(lambda x:x != temp1)
header1 = temp1.split(',')

temp2 = table2.first()
table2 = table2.filter(lambda x:x != temp2)
header = temp2.split(',')


data_extract = table1.map(lambda line: (line.split(','))) \
    .filter(lambda line: len(line) == len(header))#\
    #.map(lambda line: (line[0], line[2]))\
    #.reduceByKey(lambda x, y: x or y).cache()
'''
data_extract2 = table2.map(lambda line: (line.split(','))) \
    .filter(lambda line: len(line) == len(header))\
    .map(lambda line: (line[0], str(line[4])+str(line[5])+str(line[6])))\
    .reduceByKey(lambda x, y: x or y).cache()
'''
#both_rdd = data_extract.join(data_extract2)
#both_rdd.take(10)

#both_rdd = data_extract

data_extract.coalesce(100).saveAsTextFile('s3n://bigdives3/DataClean/Join_query')

