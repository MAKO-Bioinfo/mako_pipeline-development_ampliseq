import os
import sys
import glob
from pandas import *


#os.chdir('/Volumes/Genetics/Jack/data/hereditary_data/')
## this is a test script
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/test')

## this is where the files are
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/disease_mutation_text_files_key')
datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/scratch')


#allFiles = glob.glob('*.txt')
allFiles = glob.glob('*.csv')


complete_frame = pandas.DataFrame()
list =[]

for file in allFiles:
    fileName = os.path.splitext(file)[0]
    print 'Reading datafile: ' + fileName
    #file_names.append(fileName)
    #df = pandas.read_table(file,index_col=None)
    df = pandas.read_csv(file,index_col=None)


    df['file name'] = fileName

    # fill empty cell with NA string
    df = df.fillna('NA')

    ## Step 1. filter the rows by Associated Gene Name = 'LRG'
    #new_df = df[~df['Associated Gene Name'].str.contains(r'LRG_')]
    new_df = df[~df['associated_gene'].str.contains(r'LRG_')]

    ## Step 2. filter by Clinical significance, everything with pathogenic or risk factor
    #new_df = new_df[new_df['Clinical significance'].str.contains(r"pathogenic")]
    new_df = new_df[new_df['clinical_significance'].str.contains(r"pathogenic")]

    ## Step 3. remove duplicates, match Variant Name and Consequence specific allele
    #new_df.duplicated(['Variant Name','Consequence specific allele'])
    #new_df_drop = new_df.drop_duplicates(subset=['Variant Name','Consequence specific allele'])
    new_df_drop = new_df.drop_duplicates(subset=['refsnp_id','consequence_allele_string'])

    #print new_df
    #print df
    list.append(new_df_drop)

complete_frame = pandas.concat(list)


## Step 4. Write to a csv file
complete_frame.to_csv('complete_hereditary_panel_dataframe_filtered.csv',index=False)






