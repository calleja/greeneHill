#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import json
import requests
from collections import Counter
import itertools
import pandas as pd
import numpy as np
import copy
from datetime import datetime
import re
import sys
import configparser

config = configparser.ConfigParser()
config.read('/home/merde/Documents/greeneHill/dropbox_config.ini')
authorization = config['default']['authorization']
path = config['default']['path']

url = 'https://api.dropboxapi.com/2/files/list_folder'
# POST request; requires headers    
headers = {'Authorization': authorization,'Content-Type':'application/json'}
params = {'recursive': True, 'limit':1000, 'path':path}
r4_c = requests.post(url, headers = headers, data = json.dumps(params))
ghfc_19_20_invoices_vend = r4_c.json()


def runWhole(csvFilePath):
    test = retrieveFiles(ghfc_19_20_invoices_vend)
    files, folders = consolidateTypes(test)
    tgt = ['name', 'path_lower', 'path_display','client_modified']

    files_select_dict = [{k:v for k,v in elem.items() if k in tgt} for elem in files]

    folders_select_dict = [{k:v for k,v in elem.items() if k in tgt} for elem in folders]

#edit the dictionary to store a list for the two "path" keys
    files_select_dict_II = [{k:(v.split('/') if k in ['path_lower', 'path_display'] else v) for (k,v) in elem.items()} for elem in files_select_dict]

    folders_select_dict_II = [{k:(v.split('/') if k in ['path_lower', 'path_display'] else v) for (k,v) in elem.items()} for elem in folders_select_dict]
    
    testy = elongate(files_select_dict_II)

    #convert to pandas df
    #hjh = list of dicts
    hjh = [gh['path_display'] + [gh['client_modified']] for gh in testy]
    df = pd.DataFrame(hjh, columns = ['parent_folder0','parent_folder1','parent_folder2','parent_folder3','filename','client_modified'])

    #convert date to datetime
    df['client_modified'] = pd.to_datetime(df['client_modified'], format = '%Y-%m-%d')
    
    posdates = df['filename'].str.split(r'-|_')
    
    test = posdates.apply(dateConvert)
    ma = df['filename'].apply(paidQB)

#consolidate value-add columns
    df['paid_qb_status'] = ma
    df['dateOfActivity'] = test

    df.to_csv(csvFilePath, index = False)
    
def retrieveFiles(hiLevelJson):
    lista_entries = [] 
    #a list of two or more dicts; hiLevelJson is initial value, and will be replaced later
    cursor = hiLevelJson['cursor']
    has_more = True
    def retrieveMore(response):
    #ideally place this into a do-while loop
        url = 'https://api.dropboxapi.com/2/files/list_folder/continue'
        params = {'cursor': response}
        headers = {'Authorization': authorization,'Content-Type':'application/json'}
        r4_d = requests.post(url, headers = headers, data = json.dumps(params))
        new_results = r4_d.json()
        return(list(map(new_results.get,['has_more','cursor','entries'])))
        
    #will assume that hiLevelJson is always ['has_more'] = True
    while has_more == True:
        has_more,cursor,entries  = retrieveMore(cursor)
        lista_entries.append(entries)
    return(lista_entries)

def consolidateTypes(listOfLists):
    #parse all the results into different object classes
    #PARSE vendor_subs; consolidate all folder and file tags
    all_files = []
    all_folders = []
    def returnBoth(dic):
        #filter for different object according to .tag value
        vendor_folders = list(filter(lambda miniDic: miniDic['.tag'] == 'folder', dic))
        vendor_files = list(filter(lambda miniDic: miniDic['.tag'] == 'file', dic))
        return vendor_folders,vendor_files
    for lista in listOfLists:
        fol, fil = returnBoth(lista)
        all_files.append(fil)
        all_folders.append(fol)
        #flatten these two lists of lists
    all_folders = list(itertools.chain(*all_folders))
    all_files = list(itertools.chain(*all_files))
    return all_files,all_folders

def elongate(listOflist):
    #nest a list into a list, then unwrap the whole thing before returning a value
    #discover the max amt of elements in any list
    def flatten_list(nested_list):
    #https://gist.github.com/Wilfred/7889868
        nested_list = copy.deepcopy(nested_list)
    
        while nested_list:
            sublist = nested_list.pop(0)

            if isinstance(sublist, list):
                nested_list = sublist + nested_list
            else:
                yield sublist
            
    lol = copy.deepcopy(listOflist)
    ma = [len(file['path_display']) - 1 for file in listOflist]
    dic = Counter(ma)
    ma2 = max(dic.values())
    for k,v in dic.items():
        if v == ma2:
            ma3 = k
    def replace(ma, single_list):
        blnk = list(itertools.repeat('',ma-len(single_list)-1))
        single_list.insert(-1,blnk)
        return(single_list[1:])
    for minList in lol:
        ff = replace(ma3, minList['path_display'])
        minList['path_display'] = list(flatten_list(ff))
    return(lol)

def dateConvert(line):
    try:
        fecha = datetime.strptime('-'.join(line[0:3]),'%Y-%m-%d')
        return(fecha)
    except ValueError:
        for counter, value in enumerate(line):
            if re.search("^20",value):
                try:
                    fecha = datetime.strptime('-'.join(line[counter:counter+3]),'%Y-%m-%d')
                    return(fecha)
                except ValueError:
                    return('')
            else:
                return('')

#search for presence of QB/QBO and paid; create a flag for ea scenario
def paidQB(line):
    #return categories: paid, QB, paid and QB
    cumscore = 0 
    if re.search("qb|qbo", line, flags = re.IGNORECASE):
        cumscore +=1
    if re.search("paid", line, flags = re.IGNORECASE):
        cumscore +=2
    if cumscore == 1:
        return("contains QB")
    if cumscore == 2:
        return("contains paid")
    if cumscore == 3:
        return("contains both QB and paid")
    else:
        return("unaccounted")
