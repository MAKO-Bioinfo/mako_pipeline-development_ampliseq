from pandas import *
from sqlalchemy import *
import os
ENGINE = create_engine('postgresql+psycopg2://devstats:devstats@kona.bdx.com/newseqdb')


os.chdir('/Volumes/Genetics/Ion_Workflow')
SIR317_output_dir = os.path.join(CHP2_PIPELINE_OUTPUT_DIR, 'SIR317/')