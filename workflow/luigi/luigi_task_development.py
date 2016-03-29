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
ENGINE = create_engine('postgresql+psycopg2://seqstats:seqstats@localhost:5433/seqdb')

### This is the testing area
#os.chdir('/Users/jack/CHP2/output')
os.chdir('/Volumes/Genetics')
PIPELINE_OUTPUT_DIR = 'Ion_Workflow'
annovar_csv_path = os.path.join(PIPELINE_OUTPUT_DIR,'TSVC_variants_IonCode_0101_03042016.myanno.hg19_multianno.csv')
omim_database_path = os.path.join(PIPELINE_OUTPUT_DIR,'database','omim_new_info.csv')

## do the left join with omim df
annovar_df = DataFrame.from_csv(annovar_csv_path,index_col=False)

annovar_df['project_id'] = Series('ACC101',index=annovar_df.index)
omim_database_df = DataFrame.from_csv(omim_database_path,index_col=None)

annotated_df = DataFrame.merge(annovar_df,omim_database_df,how="left",on="Gene.refGene")
## ADD the INDEX as LIMS ID
annotated_df['project_id'] = Series('ACC101',index=annotated_df.index)
annotated_df.to_sql('annotated_df',ENGINE,if_exists='replace',index=true)

## CLEAN UP THE COLUMNS

###


