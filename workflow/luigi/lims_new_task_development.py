##===========================================================================##
## lims_new_task_development.py
##
## This is the luigi task that string all steps together
## 1. FindNewTorrentRuns
## 2. Copy
## 3. QC ---> DB
## 4. ANNOTAION ---DB
##
## Jack Yen
## March 25th, 2016
##===========================================================================##
##===========================================================================##
## All of the general library calls and PATH
##===========================================================================##
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
import numpy

import requests
import re
import sys
import errno

from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from collections import defaultdict
import pandas as pd
import requests_cache
import requests
import lims

__LIMS__ = Lims(BASEURI, USERNAME, PASSWORD)


## set a working directory
os.chdir('/Volumes/Genetics/Ion_Workflow')

TORRENT_DATA = 'ionadmin@10.0.1.74:/home/ionguest/results/analysis/output/Home'

# Constants for LIMS UDFs
LIMS_READY_PIPELINE_UDF = 'Ready For Pipeline'
LIMS_DO_NOT_ANALYSE_UDF = 'DO NOT ANALYSE'
LIMS_RUN_ID_UDF = 'Torrent Suite Planned Run - Run Code'
LIMS_TORRENT_SERVER_UDF = 'Torrent Server'
LIMS_TORRENT_REPORT_UDF = 'Torrent Suite Analysis Report'
LIMS_BARCODE_UDF = 'Sequencing Barcode'


## INPUT & OUTPUT FOLDERS
TORRENT_PIPELINE_DATA_DIR = '/Volumes/Genetics/Ion_Workflow/data'
TORRENT_PIPELINE_OUTPUT_DIR = TORRENT_PIPELINE_DATA_DIR

#SQLAlchemy postgreSQL ENGINE
ENGINE = create_engine('postgresql+psycopg2://postgres:seqstats@localhost/postgres')


#TORRENT_URL_MAP = {'iontorrent01':'http://pgm.bdx.com','iontorrent02':'http://pgm2.bdx.com'}
TORRENT_URL_MAP = {'ionadmin':'http://10.0.1.74','ionadmin':'http://10.0.1.74'}

def get_torrent_folder(torrent_server, torrent_report):
    print 'torrent report ' + str(torrent_report)
    torrent_report_num = os.path.normpath(torrent_report).rsplit('/', 1)[1]
    request_url = os.path.join(torrent_server, 'rundb/api/v1/results', torrent_report_num,
                               '?format=json')
    print 'requ: ' + request_url
    ts_api_request = requests.get(request_url, auth=('ionadmin', 'ionadmin'))

    if ts_api_request.status_code == 200:
        ts_api_response = ts_api_request.json()
        if len(ts_api_response) == 0:
            raise Exception('ERROR, no Run matching ' + torrent_report)
        try:
            folder_name = os.path.basename(os.path.normpath(ts_api_response['reportLink']))
        except:
            print "this report link has an ERROR that needs attention"
            pass
        return folder_name
    else:
        ts_api_request.raise_for_status()

filename = 'output/cron_job/log_luigi_CalculateRunTask_complete.txt'
if os.path.exists(filename):
    try:
        os.remove(filename)
    except OSError, e:
        print ("Error: %s - %s." %(e.filename,e.strerror))
else:
    print("Sorry, Cannot Find %s file." % filename)

class FindNewTorrentRunsTask(luigi.Task):

    def output(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget('output/cron_job/log_findNewRuntask_%s.txt' %(timestamp))

    def requires(self):

        ## this will query the LIMS and get all the runs


        torrent_df = lims.get_torrent_metadata()
        dfp = torrent_df.pivot(index='process_id', columns='key', values='value')
        dfdedup = torrent_df.drop_duplicates('process_id')
        dfp = dfp.join(dfdedup[['process_id', 'project_id', 'project_name']].set_index('process_id'))

        dfp['process_id'] = dfp.index
        dfp['new_project_id'] = dfp.project_name.str.replace(" ", "_") + '_' + dfp.project_id

        ## unique list of project_id
        # lims_project_list = dfdedup['project_id'].drop_duplicates().tolist()
        lims_project_list = dfp['new_project_id'].drop_duplicates().tolist()
        lims_project_list_encode = [x.encode('UTF8') for x in lims_project_list]
        local_ngs_project_list = os.listdir(TORRENT_PIPELINE_DATA_DIR)
        new_project_list = list(set(lims_project_list_encode) - set(local_ngs_project_list))
        new_project_run = pd.DataFrame(new_project_list)
        new_project_run.columns = ['new_project_id']
        new_dfp = pd.merge(dfp, new_project_run, how='inner')

        tasks = []
        for index, row in new_dfp.iterrows():
            # if ('Ready For Pipeline' in dict(row) is True):
            #     print row
            if row[LIMS_READY_PIPELINE_UDF] is True:
                # print row
                project = row['project_id']
                new_project_id = row['new_project_id']
                process_id = row['process_id']
                server = row[LIMS_TORRENT_SERVER_UDF]
                report = row[LIMS_TORRENT_REPORT_UDF]
                # if numpy.isnan(report):
                if pd.isnull(report):
                    print ("ERROR: This report has a NaN  for project %s -- Check LIMS Server!" % (project))
                    continue
                folder = get_torrent_folder(server, report)
                runid = report.rsplit('/', 1)[1]
                #print runid

                run_paths = os.path.join(TORRENT_DATA, folder)
                # run_paths = glob.glob(os.path.join(TORRENT_DATA, '*'+folder+'*'))
                # print run_paths

                # if len(run_paths) != 1:
                #     print("ERROR: " + str(len(
                #         run_paths)) + " paths found -- No Torrent Run folder, or multiple folders found matching \"" +
                #             folder + "\" on server: \"" + server + "\" -- Check Run Database!")
                #     continue
                # print 'adding ' + run_paths[0] + ' to the task list'
                print 'adding ' + run_paths + ' to the task list'
                tasks.append(CalculateRunTask(new_project_id=new_project_id,project_id=project,process_id=process_id,
                                              torrent_folder=run_paths,
                                              torrent_runid=server + '_' + runid))

        return tasks
    def run(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write(' ======= FindNewRunsTask Done! ====== {t}'.format(t=timestamp))

class CopyRunTask(luigi.task):
    new_project_id = luigi.parameter()
    project_id = luigi.parameter()
    process_id = luigi.parameter()
    torrent_folder = luigi.parameter()
    torrent_runid = luigi.parameter()

    def output(self):
        return luigi.LocalTarget(
            'output/%s/%s/log_file_CopyRunTask_%s_%s_complete.txt' % (self.new_project_id, self.process_id, self.process_id,self.torrent_runid))

    def run(self):
        target_dir = os.path.join(TORRENT_PIPELINE_OUTPUT_DIR,self.new_project_id,self.process_id, '')
        print target_dir
        outfile = self.output().open('w')
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        ## rsync -avz -e "sshpass -p ionadmin ssh -p 4040" ionadmin@10.0.1.74:/home/ionguest/results/analysis/output/Home/Auto_user_S5-00391-3-Cancer_Hotspot_Panel_v2_54_017 .

        cmd = ['rsync', '-ahv' ,'-e ','"sshpass -p ionadmin ssh -p 4040"','--no-links','--progress',
               os.path.join(self.torrent_folder, ''), target_dir]
        subprocess.call(cmd, stdout=outfile, stdin=outfile)
        print ('COPY STEP : rsync Finished!!!')
        print outfile.path
        outfile.close()


class QCMetricsTask(luigi.ExternalTask):
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    barcode    = luigi.Parameter()




class QCMetricsTableTask(luigi.task):
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    barcode = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid  = luigi.Parameter()
    def output(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget('output/log/%s_%s_%s_%s_%s_%s.txt' % (
            self.project_id, self.process_id, self.torrent_runid, self.barcode, self.table_name, 'complete'))
    def run(self):
        df = DataFrame.from_csv(self.input().path,parse_dates=False)
        connection = ENGINE.connect()

        # #===========================================================================##
        # # This will update the chip_statistics table whenever there's a code change
        # # it wiil scan row by row (each row per project_id) and delete existed row
        # # replace by the new ones
        # #===========================================================================##
        # this if for writing to chip-_statistics table to database, if yes Delete data from db using SQLAlchemy expression
        if self.table_name=="chip_statistics":
        #if self.table_name.value:'chip_statistics'
            connection.execute("DELETE from %s WHERE project_id= '%s'" %(self.table_name,self.project_id))
            connection.close()
            df.to_sql(self.table_name,ENGINE,if_exists='append',index=True)
            print "===== Update chip_statistics tables COMPLETE! ===="
        else:
         # #===========================================================================##
        # # This will update base_statistics, amplicon statistics, chip_statistics table whenever there's a code change
        # # it wiil scan row by row (each row per project_id) and delete existed row
        # # replace by the new ones
        # #===========================================================================##
            #if ENGINE.dialect.has_table(connection, self.table_name):
            #    if self.table_name : ['base_statistics','amplicon_statistics','barcode_statistics']
            connection.execute("DELETE from %s WHERE project_id= '%s' AND barcode= '%s'" %(self.table_name,self.project_id,self.barcode))
            connection.close()
            df.to_sql(self.table_name,ENGINE,if_exists='append',index=True)
            print "===== Update tables COMPLETE! ===="

        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('done at finished at {t}'.format(t=timestamp))


class CalculateRunTask(luigi.task):
    process_id = luigi.parameter()
    project_id = luigi.parameter()
    new_project_id = luigi.parameter()
    torrent_folder = luigi.parameter()
    torrent_runid  = luigi.parameter()

    def requires(self):
        # Use Process python object to get the samples for the PGM run
        #proc = Process(__LIMS__, id=self.process_id)
        project_name = Project(__LIMS__,id=self.project_id).name
        #artifact = proc.all_inputs()[0]
        #barcodes = [s.udf[LIMS_BARCODE_UDF] for s in artifact.samples]
        sample_df = lims.get_sample_udfs(name=project_name)
        barcodes = sample_df.loc[sample_df['key']=='Sequencing Barcode'].value
        # Query LIMS to get samples for the runs
        #project_name = Project(__LIMS__, id=self.project_id).name
        #project_name = Project(__LIMS__,id=self.process_id).name
        #project_name = Process(__LIMS__,id=self.process_id).id
        tasks = [CopyRunTask(new_project_id=self.new_project_id,project_id=self.project_id, process_id=self.process_id,
                             torrent_folder=self.torrent_folder,torrent_runid=self.torrent_runid)]
        for barcode in barcodes:
            print "======= The LIMS project ID: %s, process ID: %s and barcode: %s that ran :" % (self.project_id,
                                                                                                  self.process_id,
                                                                                                  barcode)
            tasks.append(
                BarcodeDBTask(data_dir=CHP2_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,project_id=self.project_id,
                                       process_id=self.process_id, barcode=barcode, table_name='barcode_statistics',
                                       torrent_folder=self.torrent_folder, torrent_runid=self.torrent_runid))
            tasks.append(
                AmpliconDBTask(data_dir=CHP2_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,project_id=self.project_id,
                               process_id=self.process_id,
                               barcode=barcode, table_name='amplicon_statistics', torrent_folder=self.torrent_folder,
                               torrent_runid=self.torrent_runid))
            tasks.append(
                BaseDBTask(data_dir=CHP2_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,project_id=self.project_id,
                           process_id=self.process_id,
                           barcode=barcode, table_name='base_statistics', torrent_folder=self.torrent_folder,
                           torrent_runid=self.torrent_runid))
            tasks.append(
                ChipDBTask(data_dir=CHP2_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,project_id=self.project_id,
                           process_id=self.process_id,
                           barcode=barcode, table_name='chip_statistics', torrent_folder=self.torrent_folder,
                           torrent_runid=self.torrent_runid))
        return tasks
    def output(self):
        ## ignore timestamp to get rid of the unfullfill error
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget('output/cron_job/log_luigi_CalculateRunTask_complete.txt')

    def run(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('Congratulations! Luigi Root Task at finished')
        print "======= CONGRATULATIONS! ALL Luigi Tasks COMPLETED! ======"




if __name__ == '__main__':
     luigi.run(main_task_cls=FindNewTorrentRunsTask)
