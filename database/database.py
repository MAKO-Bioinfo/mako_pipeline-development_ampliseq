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


ENGINE = create_engine('postgresql+psycopg2://postgres:seqstats@localhost/postgres')



os.chdir('/Volumes/Genetics/Ion_Workflow')
SIR317_output_dir = os.path.join(CHP2_PIPELINE_OUTPUT_DIR, 'SIR317/')