# -*- coding: utf-8 -*-

table1 = sc.textFile('s3n://bigdives3/DataClean/dbo.shop.header.droppedhard.csv')
table2 = sc.textFile('s3n://bigdives3/DataClean/DataClean/dbo.shop.STAT_storico_dett.droppedhard.csv')

def ExtractHeader(table):
    temp = table.first()
    table = table.filter(lambda x:x !=temp)
    header = temp.split(',')
    return table, header

table1, header1 = ExtractHeader(table1)
table2, header2 = ExtractHeader(table2)

def SanityCheck(table, header):
    data_extract = table.map(lambda line: (line.split(','))) \
        .filter(lambda line: len(line) == len(header))
    print 'Columns number: ', data_extract.count()
    return data_extract

table1 = SanityCheck(table1, header1)
table2 = SanityCheck(table2, header2)

data_extract = table2.map(lambda line: line[4]+line[5]+line[6])

aaa = data_extract.filter(lambda line: line == '19782')

aaa.count()
