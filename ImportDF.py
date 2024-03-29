import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime


def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.Name},inplace=True)
    return df



def processArchive(pathImport):
    pathImportSI = os.getcwd() + pathImport
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, on_bad_lines='skip',dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000 )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
        li.append(df)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI

def processArchive2(fields,pathImport):
    pathImportSI = os.getcwd() + pathImport
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, on_bad_lines='skip',dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000,usecols = fields )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
        df2 = df[fields] # ordering labels 
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI





def ImportDF_Xlsx(fields,fields2, pathImport,sheetname,skip_rows):
  pathImportSI = os.getcwd() + pathImport
  archiveName = pathImport[8:len(pathImport)]
  #print ('loalding files...\n')
  all_filesSI = glob.glob(pathImportSI + "/*.xlsx")
  all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
  li = []
  df = pd.DataFrame()
  for filename in all_filesSI:
    #data = pd.read_excel(filename,skiprows=27,sheet_name = 'DUMP_5G_DSS', nrows=40,usecols = 'A:AC')
    data = pd.read_excel(filename,skiprows=skip_rows,sheet_name = sheetname,usecols = fields,na_filter= False)
    frameSI = df.append(data,ignore_index=True)
    frameSI = frameSI[fields] # ordering labels 
  frameSI.columns = fields2
  frameSI = frameSI.drop_duplicates()
  frameSI = frameSI.reset_index(drop=True)
  return frameSI 
