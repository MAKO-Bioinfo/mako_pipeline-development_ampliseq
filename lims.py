# #===========================================================================##
# # lims.py
# # This is a test script to test the LIMS API python package
# #
# # Jack Yen
# # Feb 18th, 2015
# #===========================================================================##
import time
import glob
import os
import subprocess
import json

from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from collections import defaultdict
import pandas as pd
import requests_cache
import requests

## set a working directory
os.chdir('/Volumes/Genetics/Ion_Workflow')


#TORRENT_02_MOUNT = '/Volumes/Runs2'
#TORRENT_01_MOUNT = '/Volumes/Runs1'
TORRENT_DATA = 'ionadmin@10.0.1.74:/home/ionguest/results/analysis/output/Home'

__LIMS__ = Lims(BASEURI, USERNAME, PASSWORD)


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


#TORRENT_URL_MAP = {'iontorrent01':'http://pgm.bdx.com','iontorrent02':'http://pgm2.bdx.com'}
TORRENT_URL_MAP = {'ionadmin':'http://10.0.1.74','ionadmin':'http://10.0.1.74'}
#TORRENT_FOLDER_MAP = {'iontorrent01': TORRENT_01_MOUNT,'iontorrent02': TORRENT_02_MOUNT}


#project_list = __LIMS__.get_projects()

def get_projects(project_id_list=None):
    global __ENGINE__
    with requests_cache.disabled():
        if project_id_list is not None:
          projects = [Project(__LIMS__,id=pid) for pid in project_id_list]
        else:
          projects = __LIMS__.get_projects()
    return projects


def get_torrent_metadata(project_id_list=None):
    df = pd.DataFrame(columns=['project_name', 'project_id', 'process_id', 'key', 'value'])
    i = -1

    projects = get_projects(project_id_list)

    for project in projects:
        project_dict = get_torrent_procs(name=project.name)
        for k, v in project_dict.iteritems():
                for proc in v:
                    for itm in proc.udf.items():
                        i += 1
                        df.loc[i] = [project.name, project.id, proc.id, itm[0], itm[1]]
    return df


def get_torrent_procs(**kwargs):
    project_dict = defaultdict(lambda: [])
    for prj in __LIMS__.get_projects(**kwargs):
        if prj.id in project_dict:
            raise TypeError("Duplicated Project")
        nm = prj.name
        procs = __LIMS__.get_processes(projectname=nm)
        procs_unlist = [proc for proc in procs]
        for proc in procs_unlist:
        ## Filtered by TORRENT_PROCESS_TYPE
        #procs_filtered = [proc for proc in procs if is_torrent_process(proc)]
            project_dict[prj.id].append(proc)
    return project_dict

# For now, filter for Processes with a UDF "Torrent Server"
# TODO: check for "valid run" indicator, which will be a boolean UDF
# def is_torrent_process(process):
#      if int(process.type.id) == LIMS_TORRENT_PROCESS_TYPE:
#          return True
#      return False


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

def get_sample_udfs(verbose=False, **kwargs):
    """

    :rtype : pandas.DataFrame
    """
    df = pd.DataFrame(columns=['project_id', 'project_name', 'sample_id', 'sample_name', 'sample_location', 'key', 'value'])
    i = -1
    for project in __LIMS__.get_projects(**kwargs):
        samples = __LIMS__.get_samples(projectlimsid=project.id)
        for sample in samples:
            for key, value in sample.udf.items():
                i += 1
                if verbose:
                    print(' ', project.id, project.name, sample.id, sample.name, sample.artifact.location[1], key, value)
                try:
                  if str(value).decode() != "NA":
                    df.loc[i] = [project.id, project.name, sample.id, sample.name, sample.artifact.location[1], str(key).decode(), str(value).decode()]
                except:
                  print('UDF HAS INVALID TEXT FORMAT: project name = ', project.name, ', project ID = ', project.id,
                        ', sample id = ', sample.id, ', UDF/Column = ', key, ', Value = ', value)
    return df

def get_sample_udfs_fast(verbose=False, lims = __LIMS__, **kwargs):
    """

    :rtype : pandas.DataFrame
    """
    index = columns = ['project_id', 'project_name', 'sample_id', 'sample_name', 'sample_location', 'key', 'value']
    #df = pd.DataFrame(index)
    i = -1
    all_dfs = []
    for project in lims.get_projects(**kwargs):
        #print project
        samples = lims.get_samples(projectlimsid=project.id)
        #print samples
        #sample_list = [item for item in samples.udf.items()]
        sample_dfs = []
        for sample in samples:
            if len(sample.udf.items()) > 0:
                #print sample
                #sample_df = pd.DataFrame(sample.udf.items(),columns=index)
                sample_df = pd.DataFrame(sample.udf.items(),columns=['key','value'])
                sample_df['project_id'] = project.id
                sample_df['project_name'] = project.name
                sample_df['sample_id'] = sample.id
                sample_df['sample_name'] = sample.name
                sample_df['sample_location'] = sample.artifact.location[1]
                #print sample_df
                sample_dfs.append(sample_df)
        if len(sample_dfs) > 0:
            all_dfs.append(pd.concat(sample_dfs))
    if len(all_dfs) == 1:
        df = all_dfs[0]
    elif len(all_dfs) > 1:
        #Tracer()()
        df = pd.concat(all_dfs)
    else:
        df = None
    return df





#
# torrent_df = get_torrent_metadata()
# dfp = torrent_df.pivot(index='process_id', columns='key', values='value')
# dfdedup = torrent_df.drop_duplicates('process_id')
# dfp = dfp.join(dfdedup[['process_id', 'project_id', 'project_name']].set_index('process_id'))
#
# dfp['process_id'] = dfp.index
# dfp['new_project_id'] = dfp.project_name.str.replace(" ","_") + '_' + dfp.project_id
#
# ## unique list of project_id
# #lims_project_list = dfdedup['project_id'].drop_duplicates().tolist()
# lims_project_list = dfp['new_project_id'].drop_duplicates().tolist()
# lims_project_list_encode = [x.encode('UTF8') for x in lims_project_list]
# local_ngs_project_list = os.listdir(TORRENT_PIPELINE_DATA_DIR )
# new_project_list = list(set(lims_project_list_encode) - set(local_ngs_project_list))
# new_project_run = pd.DataFrame(new_project_list)
# new_project_run.columns = ['new_project_id']
# new_dfp = pd.merge(dfp,new_project_run,how='inner')
#
# tasks = []
# for index, row in new_dfp.iterrows():
#     # if ('Ready For Pipeline' in dict(row) is True):
#     #     print row
#     if row[LIMS_READY_PIPELINE_UDF] is True:
#         # print row
#         project = row['project_id']
#         new_project_id = row['new_project_id']
#         process_id = row['process_id']
#         server = row[LIMS_TORRENT_SERVER_UDF]
#         report = row[LIMS_TORRENT_REPORT_UDF]
#         # if numpy.isnan(report):
#         if pd.isnull(report):
#             print ("ERROR: This report has a NaN  for project %s -- Check LIMS Server!" % (project))
#             continue
#         folder = get_torrent_folder(server, report)
#         runid = report.rsplit('/', 1)[1]
#
#         run_paths = os.path.join(TORRENT_DATA, folder)
#         # run_paths = glob.glob(os.path.join(TORRENT_DATA, '*'+folder+'*'))
#         #print run_paths
#
#         # if len(run_paths) != 1:
#         #     print("ERROR: " + str(len(
#         #         run_paths)) + " paths found -- No Torrent Run folder, or multiple folders found matching \"" +
#         #             folder + "\" on server: \"" + server + "\" -- Check Run Database!")
#         #     continue
#         #print 'adding ' + run_paths[0] + ' to the task list'
#         print 'adding ' + run_paths + ' to the task list'
#         tasks.append(CalculateRunTask(new_project_id=new_project_id,project_id=project,process_id=process_id,
#                                               torrent_folder=run_paths[0],
#                                               torrent_runid=server + '_' + runid))
#
# return tasks
#
#
# ### ['ADM2', 'ADM51', 'ACC101', 'ACC102', 'ACC103']
#
#
# ## do a list of folder comparison here
# ## local_ngs_project_list = os.listdir(CHP2_PIPELINE_DATA_DIR )
# test_list = ['ADM2','ADM51']
#
# new_project_list = list(set(lims_project_list) - set(test_list))
# ## find the folder name
#
# ## This section can use request to get folder name
# task = []
# for index, row in dfp.iterrows():
#     project = row['project_id']
#     new_project_list = row['new_project_id']
#     process_id = row['process_id']
#
#     server = row[LIMS_TORRENT_SERVER_UDF]
#     report = row[LIMS_TORRENT_REPORT_UDF]
#     if pd.isnull(report):
#         #print ("ERROR: THIS REPORT LINK HAS A NaN")
#         continue
#
#     #print server, report
#     else:
#         folder = get_torrent_folder(server,report)
#         print folder
#
#
#
#

