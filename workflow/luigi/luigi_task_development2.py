import time
import glob
import os
import subprocess
import json

from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from pandas import *
from sqlalchemy import *

import luigi

__LIMS__ = Lims(BASEURI, USERNAME, PASSWORD)

#ENGINE = create_engine('postgresql+psycopg2://seqstats:seqstats@kona.bdx.com/seqdb')

## Have to set the port to 5433 in order to push to the POSTGRESQL DB default is 5432
## this is for local connection
#ENGINE = create_engine('postgresql+psycopg2://seqstats:seqstats@localhost:5433/seqdb')


ENGINE = create_engine('postgresql+psycopg2://seqstats:seqstats@10.0.1.79:5432/seqdb')


### This is the testing area
#os.chdir('/Users/jack/CHP2/output')
os.chdir('/Volumes/Genetics/Ion_Workflow')
PIPELINE_OUTPUT_DIR = '/Volumes/Genetics/Ion_Workflow'
annovar_csv_path = os.path.join(PIPELINE_OUTPUT_DIR,'TSVC_variants_IonCode_0101_03042016.myanno.hg19_multianno.csv')

annovar_csv_path2 = os.path.join('/Volumes/NGS/Ion_Workflow/output','NA12878_platinum_genomics.vcf.gz.avinput.myanno.hg19_multianno.csv')

annovar_csv_path3 = os.path.join('/Volumes/Genetics/Ion_Workflow/testing/GRCh37_hereditary_variants_avinput.myanno.hg19_multianno.csv')

omim_database_path = os.path.join(PIPELINE_OUTPUT_DIR,'database','omim_new_info.csv')

## do the left join with omim df
annovar_df = DataFrame.from_csv(annovar_csv_path3,index_col=False)

#annovar_df['project_id'] = Series('ACC101',index=annovar_df.index)

annovar_df.insert(0,'project_id','ACC101')
omim_database_df = DataFrame.from_csv(omim_database_path,index_col=None)

annotated_df = DataFrame.merge(annovar_df,omim_database_df,how="left",on="Gene.refGene")
## ADD the INDEX as LIMS ID
annotated_df['project_id'] = Series('ACC101',index=annotated_df.index)

annotated_df.to_csv('GRCh37_hereditary_variants.csv',index=None)

annotated_df.to_sql('annotated_variant_table',ENGINE,if_exists='replace',index=true)

## CLEAN UP THE COLUMNS


## THIS PART DOES THE QC STEP
json_qc_path = '/Volumes/Genetics/Ion_Workflow/Auto_user_S5-00391-3-Cancer_Hotspot_Panel_v2_54_017/plugin_out/coverageAnalysis_out.6/results.json'
               # json_qc_path = os.path.join(self.data_dir,self.new_project_id,self.process_id,JSON_RESULTS)
print 'this is the json qc path ',json_qc_path

with open(json_qc_path) as json_data:
        data = json.load(json_data)
            #print data
json_qc_dataframe = DataFrame.from_dict(data['barcodes']).transpose()
json_qc_dataframe.insert(0,'project_id','ACC101')
json_qc_dataframe.insert(1,'barcode_id','IonCode010')
json_qc_dataframe.to_sql('qc_table',ENGINE,if_exists='replace',index=true)



###


