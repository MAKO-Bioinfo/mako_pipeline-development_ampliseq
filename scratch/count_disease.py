import os
import sys
import glob
from pandas import *


#os.chdir(os.path.abspath('/Volumes/Genetics/Jack/data/hereditary_data/scratch/'))
## this is a test script
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/test')

## this is where the files are
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/disease_mutation_text_files_key')
datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/scratch/')

df = read_excel(os.path.join(datapath+'Mut Dis and Genes.xlsx'),index_col=None)

#df_count = df.groupby('Associated Gene Name')['file name'].count()

## this is group by the unique disease and count number of gene associated with it
df_count2 = df.groupby('file name')['Associated Gene Name'].value_counts()

df_count2.to_csv('/Volumes/Genetics/Jack/data/hereditary_data/scratch/count_gene.csv')
