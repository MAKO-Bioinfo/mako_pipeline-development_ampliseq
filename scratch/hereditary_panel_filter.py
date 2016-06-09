import os
import sys
import glob
from pandas import *
import numpy as np

#os.chdir('/Volumes/Genetics/Jack/data/hereditary_data/')
## this is a test script
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/test')

## this is where the files are
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/disease_mutation_text_files_key')
#datapath = ('/Volumes/Genetics/Jack/data/hereditary_data/Ophthal_Disease')
#datapath = ('/Volumes/Genetics/Matt/Hereditary_Panel_Version_7_aka_Small_Gene_Set/Biomart_Files_Heredi_Panel_v7')


## 6/1/16
datapath = ('/Volumes/Genetics/Jack/data/Genotypes_HuRef')

os.chdir(datapath)
allFiles = glob.glob('JCVI_*.txt')
#allFiles = glob.glob('*.csv')


complete_frame = pandas.DataFrame()
list =[]

for file in allFiles:
    fileName = os.path.splitext(file)[0]
    print 'Reading datafile: ' + fileName
    #file_names.append(fileName)
    df = pandas.read_table(file,index_col=None)
    #df = pandas.read_csv(file,index_col=None)


    df['file name'] = fileName

    # fill empty cell with NA string
    df = df.fillna('NA')

    ## Step 1. filter the rows by Associated Gene Name = 'LRG'
    #new_df = df[~df['Associated Gene Name'].str.contains(r'LRG_')]

    ## Step 2. filter by Clinical significance, everything with pathogenic or risk factor
    #new_df = new_df[new_df['Clinical significance'].str.contains(r"pathogenic")]

    #new_df = new_df[new_df['clinical_significance'].str.contains(r"pathogenic")]

    ## Step 3. remove duplicates, match Variant Name and Consequence specific allele
    #new_df.duplicated(['Variant Name','Consequence specific allele'])
    #new_df_drop = new_df.drop_duplicates(subset=['Variant Name','Consequence specific allele'])

    #new_df_drop = df.drop_duplicates(subset=['Variant Name','Phenotype description','Associated variant risk allele'])

    new_df_drop = df.drop_duplicates()
    #new_df_drop = new_df.drop_duplicates(subset=['refsnp_id','consequence_allele_string'])

    #print new_df
    #print df
    list.append(df)

complete_frame = pandas.concat(list)

rs_id = complete_frame['dbSNP RS ID']
## Step 4. Write to a csv file
#complete_frame.to_csv('complete_hereditary_panel_dataframe_filtered.csv',index=False)
complete_frame.to_csv('gene_dataframe_filtered_6_1_2016.csv',index=False)

rs_id.to_csv('Genotypes_HuRef_rs_id.csv',index=False)


## compare two dataframe list
df_5_26_16 = read_csv('/Volumes/Genetics/Jack/data/Genotypes_HuRef/5_26_16_rs_id.csv',index_col=False)
df_6_1_26  = read_csv('/Volumes/Genetics/Jack/data/Genotypes_HuRef/Genotypes_HuRef_rs_id.csv',index_col=False)

df_6_1_26_list = df_6_1_26.drop_duplicates().values.tolist()
df_5_26_16_list= df_5_26_16.drop_duplicates().values.tolist()


df_6_1_26_list_flattened = [val for sublist in df_6_1_26_list for val in sublist]
df_5_26_16_list_flattened= [val for sublist in df_5_26_16_list for val in sublist]

difference_list_rs_id= set(df_6_1_26_list_flattened) - set(df_5_26_16_list_flattened)

set(df_5_26_16_list_flattened).intersection(df_6_1_26_list_flattened)


overlap = [filter(lambda x: x in df_5_26_16_list_flattened, sublist) for sublist in df_6_1_26_list_flattened]



# overlap_list_rs_id2 = pandas.Series(list(set(df_6_1_26_list_flattened.intersection(set(df_5_26_16_list_flattened)))))
#
#
# overlap_list_rs_id2 = pandas.Series(list(set(df_6_1_26_list_flattened) & set(df_5_26_16_list_flattened)))
#


import csv

f = open('overlap_list_rs_id2.csv', "wb")
writer = csv.writer(f)

writer.writerow([overlap])
f.close()


#
# cw = csv.writer(open("difference.csv",'w'))
#
# cw.writerow(list[difference_list_rs_id])
# csv.clos
#DataFrame.from_records(difference_list_rs_id).to_csv('difference_list_rs_id.csv',index=False)



#ne = (df_6_1_26_series!=df_5_26_16_series).any(1)


from pandas.util.testing import assert_frame_equal



#difference = np.where(df_6_1_26 != df_5_26_16)

