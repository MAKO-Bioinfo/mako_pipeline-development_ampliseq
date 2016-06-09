import os
import sys
import glob
from pandas import *
import numpy as np
import vcf

datapath = ('/Volumes/Genetics/Matt/HuRef_Variants')
os.chdir('/Volumes/Genetics/Matt/HuRef_Variants')
bed_df = pandas.read_table(os.path.join(datapath,'IAD103216_197_Designed.bed'),skiprows=2,sep='\t',
                           names=['chrom','start','end','gene','space'],index_col=False)

## parse the SNP.vcf
vcf_reader = vcf.Reader(open(os.path.join(datapath,'SNP.vcf.gz'), 'r'))
#for record in vcf_reader:
#    print record.POS

## parse the INDEL.vcf
vcf_reader2 = vcf.Reader(open(os.path.join(datapath,'INDEL.vcf.gz'), 'r'))


# fetch all records on chromosome 20 from base 1110696 through 1230237
for record in vcf_reader.fetch('20',1110695, 1230237):
    print record

# SNP.vcf
with open("snp_all.txt", "w") as text_file:
    for index, row in bed_df.iterrows():
        for record in vcf_reader.fetch(row['chrom'].strip('chr'),row['start'],row['end']):

            text_file.write(str(record.CHROM)+' '+str(record.start)+' '+str(record.end)+' '+str(record.ID)+' '+str(
            record.alleles)+str(record.QUAL)+' '+str(record.INFO)+'\n')



## INDEL.vcf
with open("INDEL.txt", "w") as text_file:
    for index, row in bed_df.iterrows():
        for record in vcf_reader2.fetch(row['chrom'].strip('chr'),row['start'],row['end']):

            text_file.write(str(record.CHROM)+' '+str(record.start)+' '+str(record.end)+' '+str(record.ID)+' '+str(
            record.alleles)+str(record.QUAL)+' '+str(record.INFO)+'\n')