from sys import platform as _platform
from os import path
import numpy as np, pandas as pd
import sklearn
import sqlite3 as sql
from pandasql import sqldf


def readDB():
    if _platform == "win32":
        pathPrefix = "F:"
    elif _platform == "darwin":
        pathPrefix = "/Volumes/USB"

    dbFile = pathPrefix + "/MachineLeariningData/MyKaggle/DrawBridge/database.sqlite"

    csvcdData = pathPrefix + "/MachineLeariningData/MyKaggle/DrawBridge/cdData.csv"
    csvdData = pathPrefix + "/MachineLeariningData/MyKaggle/DrawBridge/dData.csv"
    csvcData = pathPrefix + "/MachineLeariningData/MyKaggle/DrawBridge/cData.csv"
    device_Query = '''
    SELECT  d.drawbridge_handle handle, d.device_id d_id, d.device_type d_type, d.device_os d_os, d.country d_country,
            d.anonymous_c0 d_c0, d.anonymous_c1 d_c1, d.anonymous_c2 d_c2, d.anonymous_5 d_5, d.anonymous_6 d_6, d.anonymous_7 d_7
    FROM    devices as d
    WHERE   d.drawbridge_handle <> "-1"
    '''
    cookies_Query = '''
    SELECT  c.drawbridge_handle handle, c.cookie_id c_id, c.computer_os_type c_os, c.browser_version c_brw, c.country c_country,
            c.anonymous_c0 c_c0, c.anonymous_c1 c_c1, c.anonymous_c2 c_c2, c.anonymous_5 c_5, c.anonymous_6 c_6, c.anonymous_7 c_7
    FROM    cookies as c
    WHERE   c.drawbridge_handle <> "-1"
    '''
    combine_Query = '''
    SELECT  d.drawbridge_handle handle, d.device_id d_id, d.device_type d_type, d.device_os d_os, d.country d_country,
            d.anonymous_c0 d_c0, d.anonymous_c1 d_c1, d.anonymous_c2 d_c2, d.anonymous_5 d_5, d.anonymous_6 d_6, d.anonymous_7 d_7,
            c.cookie_id c_id, c.computer_os_type c_os, c.browser_version c_brw, c.country c_country,
            c.anonymous_c0 c_c0, c.anonymous_c1 c_c1, c.anonymous_c2 c_c2, c.anonymous_5 c_5, c.anonymous_6 c_6, c.anonymous_7 c_7
    FROM    devices d INNER JOIN cookies c
    ON      d.drawbridge_handle = c.drawbridge_handle
    WHERE   d.drawbridge_handle <> "-1" AND c.drawbridge_handle <> "-1"
    '''

    ip_query = '''
    SELECT  d.drawbridge_handle handle, d.device_id d_id, d.device_type d_type, d.device_os d_os, d.country d_country,
            d.anonymous_c0 d_c0, d.anonymous_c1 d_c1, d.anonymous_c2 d_c2, d.anonymous_5 d_5, d.anonymous_6 d_6, d.anonymous_7 d_7,
            c.cookie_id c_id, c.computer_os_type c_os, c.browser_version c_brw, c.country c_country,
            c.anonymous_c0 c_c0, c.anonymous_c1 c_c1, c.anonymous_c2 c_c2, c.anonymous_5 c_5, c.anonymous_6 c_6, c.anonymous_7 c_7
    FROM    devices d INNER JOIN cookies c
    ON      d.drawbridge_handle = c.drawbridge_handle
    WHERE   d.drawbridge_handle <> "-1" AND c.drawbridge_handle <> "-1"
    '''

    with sql.connect(dbFile) as conn:
        if not path.isfile(csvdData):
            dData = pd.read_sql(device_Query, conn, index_col='handle')
            dData.to_csv(path_or_buf=csvdData)
        else:
            dData = pd.read_csv(csvdData, index_col='handle')
            dData.drop('Unnamed: 0', axis=1, inplace=True)

        if not path.isfile(csvcData):
            cData = pd.read_sql(cookies_Query, conn, index_col='handle')
            cData.to_csv(path_or_buf=csvcData)
        else:
            cData = pd.read_csv(csvcData, index_col='handle')
            cData.drop('Unnamed: 0', axis=1, inplace=True)

        if not path.isfile(csvcdData):
            cdData = pd.read_sql(combine_Query, conn, index_col='handle')
            cdData.to_csv(path_or_buf=csvcdData)
        else:
            cdData = pd.read_csv(csvcdData, index_col='handle')
            cdData.drop('Unnamed: 0', axis=1, inplace=True)

    cdData.drop(['d_type','d_os','c_os','c_brw'], axis=1, inplace=True)
    columns = list(cdData)
    dataTuple =(cdData[[s for s in columns if 'd_' in s]], cdData[[s for s in columns if 'c_' in s]])

    return dataTuple


class myDataloder(object):
    def __init__(self, filename):
        self.filename = filename

    def loadFile(self):
        pass


class myClassifier(object):
    def __init__(self, clname, Dataloader, **kwargs):
        self.classifername = clname
        self.argin = kwargs
