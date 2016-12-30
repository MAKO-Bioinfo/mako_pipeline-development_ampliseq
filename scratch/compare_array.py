import os, sys
import csv
from pandas import *

## 12/16/16
datapath = r"/Volumes/Genetics/Matt/Call Check/"
#datapath = r"/Users/jack/workspace/Call Check"
os.chdir(datapath)

# lines = []
# with open("384.txt") as f:
#     for line in f:
#         if not line.startswith("#"):
#             lines.append(line)

reader1 = read_table('384.txt',skiprows=16)
reader1['index'] = reader1.index
reader2 = read_table('Results OA.txt',skiprows=16)
reader2['index'] = reader2.index


df = reader1[~((reader1['Assay ID'].isin(reader2['Assay ID']))  & (reader1['Call'].isin(reader2['Call'])))]

reader1['Assay ID'].isin(reader2['Assay ID'])



reader1[~reader1['Assay ID'].isin(reader2['Assay ID'])]


reader1[reader1['Assay ID'].isin(reader2['Assay ID']) & reader1['Call'].isin(reader2['Call'])]


df =reader1[~(reader1['Assay ID'].isin(reader2['Assay ID']) & reader1['Sample ID'].isin(reader2['Sample ID']) & reader1[
    'Call'].isin(reader2['Call']))]

reader1[reader1['Assay ID'].isin(reader2['Assay ID'])]

df.to_csv('test.csv',header=True,index_label=True)




# if reader1['Assay ID']==reader2['Assay ID'] & reader1['Call']==reader2['Call']:
#     pass
# else:
#     print reader1
#
#
#
# for row in reader1:
#     print row
#
# reader2 = csv.reader(open('Results OA.txt','rb'))
# row1 = reader1.next()
# row2 = reader2.next()
#
# if (row1[0] == row2[0]) and (row1[2:] == row2[2:]):
#     print "eq"
# else:
#     print "different"
#
#
# f = open("384.txt")
# lines = f.readlines()[17:]
# print str(lines) + '\n'
# f.close()
#
# for line in lines:
#     print (line.strip())

