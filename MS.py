import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import warnings
warnings.simplefilter("ignore")

def processArchive():
    fields = ['Dia','Regional','RAN Node','BSC/RNC','Célula','Banda','Tecnologia','Data Primeiro Tráfego','Data Último Tráfego','Último Tráfego Voz Registrado','Último Volume Dados Registrado','Tráfego de Voz','Volume de Dados']
    fields2 = ['Dia','Regional','SITE','BSC/RNC','CELL','FREQ CELL','Tec','DataPrimeiroTrafego','DataUltimoTrafego','UltimoTrafegoVozRegistrado','UltimoVolumeDadosRegistrado','TrafegodeVoz','VolumedeDados']
    
    folder = 'MS'
    regional = 'TSP'

    pathImport = '/import/' + folder
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+folder+'/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=2, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields)
        df = pd.concat([chunk[(chunk['Regional'] == regional)] for chunk in iter_csv])
        #df = pd.concat([chunk for chunk in iter_csv])
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2


    frameSI['Dia ID1'] = pd.to_datetime(frameSI['Dia'], format="%d/%m/%Y")
    frameSI['DataPrimeiroTrafego1'] = pd.to_datetime(frameSI['DataPrimeiroTrafego'], format="%d/%m/%Y")
    frameSI['DataUltimoTrafego1'] = pd.to_datetime(frameSI['DataUltimoTrafego'], format="%d/%m/%Y")
    
    frameSI['DaysOff'] = (frameSI['Dia ID1'] - frameSI['DataUltimoTrafego1']).dt.days
    frameSI.loc[frameSI['DaysOff'] < 0,['DaysOff']] = 0
    frameSI['ActiveCellTime(days)'] = (frameSI['DataUltimoTrafego1'] - frameSI['DataPrimeiroTrafego1']).dt.days
    frameSI = frameSI.drop(['Dia ID1','DataPrimeiroTrafego1','DataUltimoTrafego1'],1)

    ''' Corrigir para avaliar o trafego
    #'UltimoTrafegoVozRegistrado','UltimoVolumeDadosRegistrado','TrafegodeVoz','VolumedeDados'
    frameSI['STATUS'] = 'INATIVO'
    frameSI.loc[(frameSI['UltimoTrafegoVozRegistrado'].replace(',','.').astype(float) > 0)|
                (frameSI['UltimoVolumeDadosRegistrado'].replace(',','.').astype(float) > 0)|
                (frameSI['TrafegodeVoz'].replace(',','.').astype(float) > 0)|
                (frameSI['VolumedeDados'].replace(',','.').astype(float) > 0)&
                (frameSI['DaysOff'] == 0),['STATUS']] = 'ATIVO'
    '''
    frameSI.loc[frameSI['DaysOff'] == 0,['STATUS']] = 'ATIVO'
    frameSI.loc[frameSI['DaysOff'] > 0,['STATUS']] = 'INATIVO'


    #frameSI.loc[(frameSI['Tec'] == '5G'),['Tec']] = '5G'
    

    frameSI.loc[~(frameSI['SITE'].str[:3] == '5G-') & (frameSI['Tec'] == '5G'),['FREQ CELL']] = frameSI['FREQ CELL'].astype(str)+'DSS'
    


    
    return frameSI
#Baixar arquivo como csv, alterAR CODIGO