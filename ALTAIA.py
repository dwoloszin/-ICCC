import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import warnings
import RemoveDuplcates
warnings.simplefilter("ignore")

def processArchive():
    fields = ['Cell',' TIM_DISP_COUNTER_TOTAL (%)',' TIM_VOLUME_DADOS_DLUL_ALLOP (KB)',' TIM_TRAFEGO_VOZ_ALLOP (E)']
    fields2 = ['CELL','DISP','VOLUME','TRAFEGO']
    folder = 'ALTAIA'

    pathImport = '/import/' + folder
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[11:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+folder+'/'+folder+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=0, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ',',iterator=True, chunksize=10000, usecols = fields)
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2

    #Removing duplicates, keep last
    frameSI.insert(len(frameSI.columns),'STATUS',0)
    frameSI.loc[(frameSI['DISP'].astype(float) > 0) | ((frameSI['VOLUME'].astype(float) > 0) | (frameSI['TRAFEGO'].astype(float) > 0)),['STATUS']] = 1
    
    frameSI = RemoveDuplcates.processarchive(frameSI,'CELL','STATUS')
    frameSI['STATUS'] = frameSI['STATUS'].map({0:'SEM TRAFEGO',1:'ATIVO'})
    

    
    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO