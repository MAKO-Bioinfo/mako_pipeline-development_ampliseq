##===========================================================================##
## lims_new_task_development.py
##
## This is the luigi task that string all steps together
## 1. FindNewTorrentRuns
## 2. Copy
## 3. QC ---> QC Database
## 4. ANNOTATION ---> Annotation Database
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
from pandas.io.json import json_normalize

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
import json
import csv

from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from collections import defaultdict
import pandas as pd
import requests_cache
import requests

## import the lims.py file that has all the functions
import lims

__LIMS__ = Lims(BASEURI, USERNAME, PASSWORD)

## SLACK
#from slacker import Slacker


## set a working directory
#os.chdir('/Volumes/Genetics/Ion_Workflow')

# Initialization
os.system('mkdir %s' % '/Volumes/Genetics')
os.system('mkdir %s' % '/Volumes/NGS')
# os.system('mkdir /Volumes/NGS')
#os.system('mount -t smbfs //GUEST:@pgm.bdx.com/Runs %s' % TORRENT_01_MOUNT)
#os.system('mount -t smbfs //GUEST:@pgm2.bdx.com/Runs %s' % TORRENT_02_MOUNT)

os.system("mount_smbfs //'mako;jyen:qazwsx0305!!'@10.0.1.12/Genetics /Volumes/Genetics")
os.system("mount_smbfs //jyen:MakoGen1@makogene/jyen /Volumes/NGS")

TORRENT_DATA = 'ionadmin@10.0.1.74:/home/ionguest/results/analysis/output/Home'

# Constants for LIMS UDFs
LIMS_READY_PIPELINE_UDF = 'Ready For Pipeline'
LIMS_DO_NOT_ANALYSE_UDF = 'DO NOT ANALYSE'
LIMS_RUN_ID_UDF = 'Torrent Suite Planned Run - Run Code'
LIMS_TORRENT_SERVER_UDF = 'Torrent Server'
LIMS_TORRENT_REPORT_UDF = 'Torrent Suite Analysis Report'
LIMS_BARCODE_UDF = 'Sequencing Barcode'

## PATHS TO JSON file for QC metrics
JSON_RESULTS             = 'plugin_out/coverageAnalysis_out*/results.json'
VCF_GLOB                 = 'plugin_out/variantCaller_out*/{0}/TSVC_variants.genome.vcf'


# os.chdir('/Volumes/NGS/Ion_Workflow/')
os.chdir('/home/jyen/Ion_Workflow')
## INPUT & OUTPUT FOLDERS THIS IS RUNNING LOCALLY
# TORRENT_PIPELINE_DATA_DIR = '/Volumes/NGS/Ion_Workflow/data'
# TORRENT_PIPELINE_OUTPUT_DIR = '/Volumes/NGS/Ion_Workflow/output'
# TORRENT_PIPELINE_DATABASE_DIR = '/Volumes/NGS/Ion_Workflow/database'

## INPUT & OUTPUT FOLDERS
TORRENT_PIPELINE_DATA_DIR = '/home/jyen/Ion_Workflow/data'
TORRENT_PIPELINE_OUTPUT_DIR = '/home/jyen/Ion_Workflow/output'
TORRENT_PIPELINE_DATABASE_DIR = '/home/jyen/Ion_Workflow/database'


#SQLAlchemy postgreSQL ENGINE
## this is for local connection
#ENGINE = create_engine('postgresql+psycopg2://seqstats:seqstats@localhost:5433/seqdb')

ENGINE = create_engine('postgresql+psycopg2://postgres:seqstats@10.0.1.79:5432/seqdb')

## TEST DATABASE called TORRENT
#ENGINE = create_engine('postgresql+psycopg2://postgres:seqstats@10.0.1.79:5432/torrent')


sys.path.append('/home/jyen/workspace/mako_pipeline-development_ampliseq')

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
        return luigi.LocalTarget('output/cron_job/log_findNewRuntask_%s.txt'%(timestamp))

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
        print local_ngs_project_list
        print 'Finish query the LIMS to find new runs'
        for index, row in new_dfp.iterrows():
            # if ('Ready For Pipeline' in dict(row) is True):
            #     print row
            ## THIS IS WHERE TO UTILIZE READY FOR PIPELINE UDF
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
                run_paths = os.path.join(TORRENT_DATA, folder)

                #print runid
                # if len(run_paths)!=1:
                #     print("ERROR: " + str(len(
                #         run_paths)) + " paths found -- No Torrent Run folder, or multiple folders found matching \"" +
                #           folder + "\" on server: \"" + server + "\" -- Check Run Database!")
                #     continue
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
                                              torrent_runid=runid))
                timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
                with self.output().open('w') as outfile:
                    outfile.write(' ======= FindNewRunsTask Done! ====== {t}'.format(t=timestamp))

        return tasks


class CopyRunTask(luigi.Task):
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid = luigi.Parameter()


    ## Copy Task cannot depend on any tasks
    # def requires(self):
    #     return FindNewTorrentRunsTask(new_project_id=self.new_project_id,project_id=self.project_id,
    #                     process_id=self.process_id,torrent_folder=self.torrent_folder,
    #                     torrent_runid=self.torrent_runid)
    #

    def output(self):
        return luigi.LocalTarget('output/%s/%s/log_file_CopyRunTask_complete.txt' % (self.new_project_id, self.process_id))

    def run(self):
        target_dir = os.path.join(TORRENT_PIPELINE_DATA_DIR,self.new_project_id,self.process_id, '')
        print target_dir
        outfile = self.output().open('w')
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        ## rsync -avz -e "sshpass -p ionadmin ssh -p 4040" ionadmin@10.0.1.74:/home/ionguest/results/analysis/output/Home/Auto_user_S5-00391-3-Cancer_Hotspot_Panel_v2_54_017 .

        #cmd = ['rsync -ahv -e "sshpass -p ionadmin ssh -p 4040"',self.torrent_folder,target_dir]

        cmd2 = ['rsync -ahv --no-links --progress --exclude=*bam --exclude=*fastq ' \
               '--exclude=*fasta --exclude=sigproc_results -e ' \
               '"sshpass -p ionadmin ssh -p 4040"'+' '+os.path.join(self.torrent_folder, '')+' '+target_dir]
        cmd3 = ['rsync','-ahv','-e','"sshpass','-p','ionadmin','ssh','-p','4040"',self.torrent_folder,target_dir]
        print 'Executing the rsync command ',cmd2
        #print 'this is cmd3 ',cmd3
        subprocess.call(cmd2,stdout=outfile,stdin=outfile,shell=True)
        #(out, err) = proc.communicate()
        #subprocess.call(cmd, stdout=outfile, stdin=outfile)
        print ('COPY STEP COMPLETE : rsync Finished!!!')
        print outfile.path
        outfile.close()

# class TableTask(luigi.Task):
#     data_dir = luigi.Parameter()
#     new_project_id = luigi.Parameter()
#     project_id = luigi.Parameter()
#     process_id = luigi.Parameter()
#     barcode = luigi.Parameter()
#     table_name = luigi.Parameter()
#     torrent_folder = luigi.Parameter()
#     torrent_runid  = luigi.Parameter()
#     def output(self):
#         timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
#         return luigi.LocalTarget('output/log/%s_%s_%s_%s_%s_%s.txt' % (
#             self.project_id, self.process_id, self.torrent_runid, self.barcode, self.table_name, 'complete'))
#     def run(self):
#         df = read_csv(self.input().path,parse_dates=False, encoding='utf-8',error_bad_lines=False)
#         connection = ENGINE.connect()
#         if ENGINE.has_table(self.table_name)==True:
#            connection.execute("DELETE from %s WHERE project_id= '%s' AND barcode_id= '%s'" %(self.table_name,
#                                                                                     self.project_id,self.barcode))
#         else:
#             connection.execute("CREATE TABLE %s()" %(self.table_name))
#         connection.close()
#         df.to_sql(self.table_name,ENGINE,if_exists='append',index=False)
#
#         # #===========================================================================##
#         # # This will update base_statistics, amplicon statistics, chip_statistics table whenever there's a code change
#         # # it wiil scan row by row (each row per project_id) and delete existed row
#         # # replace by the new ones
#         # #===========================================================================##
#


class QCMetricsTask(luigi.ExternalTask):
    data_dir = luigi.Parameter()
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    #barcode    = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid = luigi.Parameter()
    def requires(self):
        return CopyRunTask(new_project_id=self.new_project_id,project_id=self.project_id,
                        process_id=self.process_id,torrent_folder=self.torrent_folder,
                        torrent_runid=self.torrent_runid)

    def output(self):
        return luigi.LocalTarget('output/%s/%s/%s_%s_%s_QC_table.csv' % (self.new_project_id, self.process_id,
                                                                     self.project_id,
                                                                     self.process_id, self.torrent_runid))

    def run(self):
        ## this is a testing area
        ##
        # json_qc_path = '/Volumes/Genetics/Ion_Workflow/Auto_user_S5-00391-3-Cancer_Hotspot_Panel_v2_54_017/plugin_out' \
        #                '/coverageAnalysis_out.6/results.json'
        json_qc_path = glob.glob(os.path.join(self.data_dir,self.new_project_id,self.process_id,JSON_RESULTS))
        json_qc_path_str = str(json_qc_path).strip('[]').replace("'","")
        print 'this is json qc path before strip',json_qc_path
        print 'this is the json qc path ',json_qc_path_str

        with open(json_qc_path_str) as json_data:
            data = json.load(json_data)
            #print data
        json_qc_dataframe = DataFrame.from_dict(data['barcodes']).transpose()
        json_qc_dataframe.insert(0,'project_id',self.project_id)
        #barcode_id = json_qc_dataframe['Alignments'].split('_')[1]
        json_qc_dataframe.insert(1,'barcode_id',json_qc_dataframe.index)
        #json_qc_dataframe.reset_index(level=0,inplace=True)


        ## write to a csv file as a record
        json_qc_dataframe.to_csv(self.output().path, header=True, index=False)
        print "======= QCMetrics_Task writing to QC_table.csv COMPLETE! ======"

        #json_qc_dataframe.to_sql('qc_table',ENGINE,if_exists='replace',index=true)

class QCMetricsDBTask(luigi.Task):
    data_dir = luigi.Parameter()
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    #barcode = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid  = luigi.Parameter()
    def requires(self):
        return QCMetricsTask(data_dir=self.data_dir,new_project_id=self.new_project_id,project_id=self.project_id,
                             process_id=self.process_id,
                             table_name='qc_table', torrent_folder=self.torrent_folder,
                             torrent_runid=self.torrent_runid)
    def output(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget('output/log/%s_%s_%s_%s_%s.txt' % (
            self.project_id, self.process_id, self.torrent_runid, self.table_name, 'complete'))

    def run(self):
        df = read_csv(self.input().path,parse_dates=False, encoding='utf-8',error_bad_lines=False)
        # connection = ENGINE.connect()
        # if ENGINE.has_table(self.table_name)==True:
        #    connection.execute("DELETE from %s WHERE project_id= '%s' AND barcode_id= '%s'" %(self.table_name,
        #                                                                             self.project_id,self.barcode))
        #    connection.close()
        #    df.to_sql(self.table_name,ENGINE,if_exists='append',index=True)
        # if ENGINE.has_table(self.table_name)==False:
        #    connection.close()
        #    #connection.execute("CREATE TABLE %s(project_id varchar(255),barcode_id varchar(255))" %(self.table_name))
        df.to_sql(self.table_name,ENGINE,if_exists='replace',index=False)

        #df.to_sql(self.table_name,ENGINE,if_exists='append',index=False)
        print "===== Update QC tables COMPLETE! ===="
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('done at finished at {t}'.format(t=timestamp))


class VariantAnnotationInputTask(luigi.ExternalTask):
    data_dir = luigi.Parameter()
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    barcode    = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid = luigi.Parameter()
    #os.chdir('/Volumes/NGS/Ion_Workflow/output')

    def requires(self):
        return CopyRunTask(new_project_id=self.new_project_id,project_id=self.project_id,
                           process_id=self.process_id,torrent_folder=self.torrent_folder,
                           torrent_runid=self.torrent_runid)
    def output(self):
        # return luigi.LocalTarget('output/%s/%s/%s_%s_%s_variant_table.csv' % (self.new_project_id, self.process_id,
        #                                                           self.project_id, self.process_id,self.barcode))
        return luigi.LocalTarget('output/%s/%s/log_file_VariantAnnotationInputTask_%s_complete.txt' % (
                                    self.new_project_id,self.process_id,self.barcode))
    def run(self):
        #outfile = self.output().open('w')
        ## this is a testing area
        # vcf_path = '/Volumes/Genetics/Ion_Workflow/Auto_user_S5-00391-3-Cancer_Hotspot_Panel_v2_54_017/plugin_out' \
        #            '/variantCaller_out.8/IonCode_0101/TSVC_variants.genome.vcf'

        #os.chdir(os.path.join(TORRENT_PIPELINE_OUTPUT_DIR,self.new_project_id,self.process_id))
        #outfile = self.output().open('w')
        vcf_glob = VCF_GLOB.format(self.barcode)
        vcf_glob_path = glob.glob(os.path.join(self.data_dir,self.new_project_id,self.process_id,vcf_glob))[0]

        vcf_path_glob_str = str(vcf_glob_path).strip('[]').replace("'","")
        print 'this is the vcf path ',vcf_glob_path
        #print vcf_path_glob_str

        ## command to run ANNOVAR
        ## 1. convert vcf to input format
        cmd_input = ['/usr/local/ngs/annovar/convert2annovar.pl',
                     '-format',
                     'vcf4',
                     vcf_path_glob_str,
                     '-outfile',
                     os.path.join(TORRENT_PIPELINE_OUTPUT_DIR,self.new_project_id,self.process_id,
                                  self.barcode+'input.avinput')]
        print 'executing ANNOVAR step1 ',cmd_input
        #subprocess.call(cmd_input)

        with self.output().open('w') as outfile:
            subprocess.call(cmd_input,stdout=outfile,stderr=outfile)
            outfile.write('ANNOTATION STEP 1 COMPLETE : ANNOVAR Input Finished!!!')
        print "======= ANNOTATION STEP 1 COMPLETE : ANNOVAR Input Finished!!! ======"


class VariantAnnotationExecuteTask(luigi.ExternalTask):
    data_dir = luigi.Parameter()
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    barcode    = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid = luigi.Parameter()
    def requires(self):
        return VariantAnnotationInputTask(data_dir=self.data_dir, new_project_id=self.new_project_id,
                                      project_id=self.project_id,
                                      process_id=self.process_id,
                        barcode=self.barcode, table_name='annotated_variant_table', torrent_folder=self.torrent_folder,
                        torrent_runid=self.torrent_runid)
    def output(self):
        return luigi.LocalTarget('output/%s/%s/log_file_VariantAnnotationExecuteTask_%s_complete.txt' % (
                                    self.new_project_id,self.process_id,self.barcode))
    def run(self):
        #outfile = self.output().open('w')
        ## this is a testing area
        # vcf_path = '/Volumes/Genetics/Ion_Workflow/Auto_user_S5-00391-3-Cancer_Hotspot_Panel_v2_54_017/plugin_out' \
        #            '/variantCaller_out.8/IonCode_0101/TSVC_variants.genome.vcf'

        ## command to run ANNOVAR
        ## 2. run annovar with parameter

        cmd_execute = ['/usr/local/ngs/annovar/table_annovar.pl',
                       self.barcode+'input.avinput',
                       '/usr/local/ngs/annovar/humandb/',
                        '-buildver',
                       'hg19',
                       '-out',
                       self.barcode+'_myannovar_output',
                       '-remove',
                       '-protocol',
                       'refGene,knowngene,ensgene,cytoBand,genomicSuperDups,snp138,cg46,popfreq_all_20150413,ljb26_all,clinvar_20160302,cosmic70,nci60',
                       '-operation',
                       'g,g,g,r,r,f,f,f,f,f,f,f',
                       '-nastring',
                       '.',
                       '-csvout']
        print 'executing ANNOVAR step2 ',cmd_execute
        ## call the two bash command from python
        #subprocess.call(cmd_execute,cwd=os.path.join('output',self.new_project_id,self.process_id))
        #subprocess.check_output(cmd_execute)

        # # read all annotated annovar output csv files into a dataframe
        # allFile = glob.glob(os.path.join('*multianno.csv'))
        # print 'these are all the annocated csv ',allFile
        # vcf_complete_frame = pandas.DataFrame()
        # list =[]
        # for file in allFile:
        #     df = read_csv(file,index_col=None)
        #     list.append(df)
        # vcf_complete_frame = pandas.concat(list)
        # vcf_complete_frame.to_csv(self.output().path, header=True, index=False)

        with self.output().open('w') as outfile:
            subprocess.call(cmd_execute,stdout=outfile,stderr=outfile,cwd=os.path.join('output',self.new_project_id,self.process_id))
            outfile.write('ANNOTATION STEP 2 COMPLETE : ANNOVAR Execution Finished!!!')
        print "======= ANNOTATION STEP 2 COMPLETE : ANNOVAR Execution Finished!!! ======"


        #print ('ANNOTATION STEP 2 COMPLETE : ANNOVAR Execution Finished!!!')
        #print outfile.path
        #outfile.close()

## this reads the ANNOVAR generated csv files
class VariantAnnotationDataFrameTask(luigi.Task):
    data_dir = luigi.Parameter()
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    barcode    = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid = luigi.Parameter()
    def requires(self):
        return VariantAnnotationExecuteTask(data_dir=self.data_dir, new_project_id=self.new_project_id,
                                      project_id=self.project_id,
                                      process_id=self.process_id,
                        barcode=self.barcode,table_name='annotated_variant_table', torrent_folder=self.torrent_folder,
                        torrent_runid=self.torrent_runid)
    def output(self):
        return luigi.LocalTarget('output/%s/%s/%s_%s_annotated_variant.csv' % (self.new_project_id,self.process_id,
                                                                          self.project_id,self.process_id))

    def run(self):
        ## this is the OMIM database information
        omim_database_path = os.path.join(TORRENT_PIPELINE_DATABASE_DIR,'omim_new_info_modified.csv')
        omim_database_df = DataFrame.from_csv(omim_database_path,index_col=None)

        # read all annotated annovar output csv files into a dataframe
        allFile = glob.glob(os.path.join('output',self.new_project_id,self.process_id,'*multianno.csv'))
        print 'these are all the annotated csv files ',allFile
        vcf_complete_frame = pandas.DataFrame()
        list =[]
        for file in allFile:
            barcode_id = os.path.splitext(file)[0].split('/')[3].replace('_myannovar_output.hg19_multianno','')
            df = read_csv(file,index_col=None,sep=None,engine='python')
            df.replace('.','NaN',inplace=True)
            df['barcode_id'] = barcode_id
            list.append(df)
        vcf_complete_frame = pandas.concat(list)
        #vcf_complete_frame.insert(0,'project_id',self.project_id)
        #vcf_complete_frame.to_csv(self.output().path, header=True, index=False)
        #vcf_complete_frame.to_csv('test.csv',header=True,index=False)
        annotated_df = DataFrame.merge(vcf_complete_frame,omim_database_df,how="left",on="Gene.refGene")
        annotated_df.insert(0,'project_id',self.project_id)
        #annotated_df.replace(regex=r"( +\.)|#", value=pd.np.NaN, inplace=True)
        annotated_df.to_csv(self.output().path, header=True, index=False)
        #annotated_df.to_sql('annotated_variant_table',ENGINE,if_exists='replace',index=true)

class VariantAnnotationDBTask(luigi.Task):
    data_dir = luigi.Parameter()
    new_project_id = luigi.Parameter()
    project_id = luigi.Parameter()
    process_id = luigi.Parameter()
    barcode = luigi.Parameter()
    table_name = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid  = luigi.Parameter()

    def requires(self):
        return VariantAnnotationDataFrameTask(data_dir=self.data_dir,new_project_id=self.new_project_id,project_id=self.project_id,
                             process_id=self.process_id,
                             barcode=self.barcode,table_name='annotated_variant_table',
                             torrent_folder=self.torrent_folder,torrent_runid=self.torrent_runid)
    def output(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget('output/log/%s_%s_%s_%s_%s.txt' % (
            self.project_id, self.process_id, self.torrent_runid, self.table_name, 'complete'))

    def run(self):
        df = read_csv(self.input().path,parse_dates=False, encoding='utf-8',error_bad_lines=False)
        # connection = ENGINE.connect()
        # if ENGINE.has_table(self.table_name)==True:
        #    connection.execute("DELETE from %s WHERE project_id= '%s'" %(self.table_name, self.project_id))
        #    connection.close()
        #    df.to_sql(self.table_name,ENGINE,if_exists='append',index=False)
        #
        # if ENGINE.has_table(self.table_name)==False:
        #    connection.close()
        df.to_sql(self.table_name,ENGINE,if_exists='replace',index=False)
        #df.to_sql(self.table_name,ENGINE,if_exists='replace',index=False)

        print "=== Update annotated_variant_table COMPLETE ==="
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('done at finished at {t}'.format(t=timestamp))

class CalculateRunTask(luigi.ExternalTask):
    process_id = luigi.Parameter()
    project_id = luigi.Parameter()
    new_project_id = luigi.Parameter()
    torrent_folder = luigi.Parameter()
    torrent_runid  = luigi.Parameter()

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
            # tasks.append(
            #     QCMetricsDBTask(data_dir=TORRENT_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,
            #                     project_id=self.project_id,
            #                     process_id=self.process_id, barcode=barcode, table_name='qc_table',
            #                     torrent_folder=self.torrent_folder, torrent_runid=self.torrent_runid))
            # tasks.append(
            #     VariantAnnotationDBTask(data_dir=TORRENT_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,
            #                             project_id=self.project_id,
            #                             process_id=self.process_id,
            #                             barcode=barcode, table_name='annotated_variant_table',
            #                             torrent_folder=self.torrent_folder,
            #                             torrent_runid=self.torrent_runid))
            tasks.append(QCMetricsDBTask(data_dir=TORRENT_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,
                                project_id=self.project_id,
                                 process_id=self.process_id,table_name='qc_table',
                                 torrent_folder=self.torrent_folder, torrent_runid=self.torrent_runid))

            tasks.append(VariantAnnotationDBTask(data_dir=TORRENT_PIPELINE_DATA_DIR,new_project_id=self.new_project_id,
                                        project_id=self.project_id,
                                         process_id=self.process_id, barcode=barcode,
                                         table_name='annotated_variant_table',
                                         torrent_folder=self.torrent_folder,torrent_runid=self.torrent_runid))
        return tasks

    def output(self):
        ## ignore timestamp to get rid of the unfullfill error
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget('output/cron_job/log_luigi_CalculateRunTask_complete.txt')

    def run(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('Congratulations! Luigi Root Task all finished{t}'.format(t=timestamp))
        print "======= CONGRATULATIONS! ALL Luigi Tasks COMPLETED! ======"
        #slack.chat.post_message('#torrent_s5_pipeline', 'Hello! Bioinformatics pipeline Finish!')


if __name__ == '__main__':
     luigi.run(main_task_cls=FindNewTorrentRunsTask)
     #luigi.run(main_task_cls=QCMetricsTask)