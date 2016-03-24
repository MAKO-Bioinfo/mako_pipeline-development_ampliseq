# #===========================================================================##
# # lims.py
# # This is a test script to test the LIMS API python package
# #
# # Jack Yen
# # Feb 18th, 2015
# #===========================================================================##

from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from collections import defaultdict
import pandas as pd
import requests_cache
import requests
import os

__LIMS__ = Lims(BASEURI, USERNAME, PASSWORD)


#TORRENT_URL_MAP = {'iontorrent01':'http://pgm.bdx.com','iontorrent02':'http://pgm2.bdx.com'}
TORRENT_URL_MAP = {'ionadmin':'http://10.0.1.74','ionadmin':'http://10.0.1.74'}

#TORRENT_FOLDER_MAP = {'iontorrent01': TORRENT_01_MOUNT,'iontorrent02': TORRENT_02_MOUNT}


project_list = __LIMS__.get_projects()

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
    request_url = os.path.join(TORRENT_URL_MAP[torrent_server], 'rundb/api/v1/results', torrent_report_num,
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



torrent_df = get_torrent_metadata()
dfp = torrent_df.pivot(index='process_id', columns='key', values='value')
dfdedup = torrent_df.drop_duplicates('process_id')
#dfp = dfp.join(dfdedup[['process_id', 'project_id', 'project_name']].set_index('process_id'))

## unique list of project_id
lims_project_list = dfdedup['project_id'].drop_duplicates().tolist()

### ['ADM2', 'ADM51', 'ACC101', 'ACC102', 'ACC103']


## do a list of folder comparison here
## local_ngs_project_list = os.listdir(CHP2_PIPELINE_DATA_DIR )
test_list = ['ADM2','ADM51']

new_project_list = list(set(lims_project_list) - set(test_list))
