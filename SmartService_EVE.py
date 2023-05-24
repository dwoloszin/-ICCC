import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import datetime 
import unique
import Count
from datetime import date

def processArchive():
    #fields = ['Número','END ID','NE ID','Status','Horário Criação','Encerrado','Alarme']
    fields = ['number','u_endereco_id','u_id','state','sys_created_on','closed_at','u_alarme']
    fields2 = ['Numero','END ID','NE ID','Status','Data de Criacao','DataFim','Alarme','dataArchive']
    

    today = date.today()
    dt = today.strftime("%d/%m/%Y %H:%M")
    print(dt)

    pathImport = '/import/SmartService'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/SmartService_EVE/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    dataArchive2 =[]
    for filename in all_filesSI:
        dataArchive = datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y%m%d')
        dataArchive2.append(filename[len(pathImportSI)+1:len(filename)-10])
        print(dataArchive2)
        iter_csv = pd.read_csv(filename, index_col=None, header=0, encoding="ANSI", error_bad_lines=False,dtype=str, sep = ',',iterator=True, chunksize=10000, usecols = fields )
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        df2.insert(len(df2.columns),'dataArchive',dataArchive)
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()
    frameSI.columns = fields2

    #Clean Alarme
    list_removing = ['&',';','*','%','|','	']
    for i in list_removing:
      frameSI['Alarme'] = frameSI['Alarme'].str.replace(i,'')

    

    
    frameSI.loc[frameSI['DataFim'].isnull(),['DataFim']] = dt

    frameSI.insert(len(frameSI.columns),'DataInicio1',pd.to_datetime(frameSI['Data de Criacao'].astype(str).str[:16], format="%d/%m/%Y %H:%M"))
    frameSI.insert(len(frameSI.columns),'DataFim1',pd.to_datetime(frameSI['DataFim'].astype(str).str[:16], format="%d/%m/%Y %H:%M"))
    
    frameSI.insert(len(frameSI.columns),'ActiveTime',(frameSI['DataFim1'] - frameSI['DataInicio1']))

    #frameSI.insert(len(df2.columns),'ActiveTime_All',pd.to_datetime('00:00:00', format="%H:%M:%S"))

    frameSISumTime = frameSI.copy()
    KeepList2 = ['END ID', 'ActiveTime']
    locationBase_top = list(frameSISumTime.columns)
    res = list(set(locationBase_top)^set(KeepList2))
    
    frameSISumTime = frameSISumTime.drop(res,axis=1)
    frameSISumTime = frameSISumTime.groupby(['END ID']).ActiveTime.sum().reset_index()
    frameSISumTime.rename(columns={'END ID': 'END ID_Sum','ActiveTime':'ActiveTime_Sum'},inplace=True)
    frameSISumTime = frameSISumTime.reset_index(drop=True)

    frameSISite = frameSI.copy()
    KeepList2 = ['NE ID', 'ActiveTime']
    locationBase_top = list(frameSISite.columns)
    res = list(set(locationBase_top)^set(KeepList2))
    
    frameSISite = frameSISite.drop(res,axis=1)
    frameSISite = frameSISite.groupby(['NE ID']).ActiveTime.sum().reset_index()
    frameSISite.rename(columns={'NE ID': 'NE ID_Sum','ActiveTime':'ActiveTime_Sum'},inplace=True)
    frameSISite = frameSISite.reset_index(drop=True)

    frameSI['Data de Criacao'] = frameSI['Data de Criacao'] + '(' + frameSI['ActiveTime'].astype(str).str[:-10] +')'
    
    
    frameSI = frameSI.sort_values(['DataInicio1'], ascending = [True])
    dataInicio = frameSI['DataInicio1'].astype(str).iloc[1]
    dataFim = frameSI['DataInicio1'].astype(str).iloc[-1]
    periodo = dataInicio + ' a ' + dataFim

    frameSI = frameSI.sort_values(['END ID','DataInicio1'], ascending = [True, True])
    frameSI = frameSI.drop(['DataFim','DataFim1','DataInicio1'],1)

    frameSI2 = frameSI.copy()
    frameSI2 = frameSI2.loc[frameSI2['Status'].isin(['Pendente','Novo','Não Iniciado','Iniciado','Validar','Relacionado'])]
    frameSI2 = frameSI2.reset_index(drop=True)

    frameSICount = Count.count(frameSI,'END ID')
    frameSI2Count = Count.count(frameSI2,'END ID')


    frameSI = frameSI.fillna('').groupby(['END ID'], as_index=True).agg('|'.join)
    removefromloop = []
    locationBase_top = list(frameSI.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    for i in res: 
        for index, row in frameSI.iterrows():
            frameSI.at[index, i] = '|'.join(unique.unique_list(frameSI.at[index, i].split('|')))
    frameSI = pd.merge(frameSI,frameSICount, how='left',left_on=['END ID'],right_on=['END ID'])



    frameSI2 = frameSI2.fillna('').groupby(['END ID'], as_index=True).agg('|'.join)
    removefromloop2 = []
    locationBase_top2 = list(frameSI2.columns)
    res = list(set(locationBase_top2)^set(removefromloop2))
    for i in res: 
        for index, row in frameSI2.iterrows():
            frameSI2.at[index, i] = '|'.join(unique.unique_list(frameSI2.at[index, i].split('|')))
    frameSI2 = pd.merge(frameSI2,frameSI2Count, how='left',left_on=['END ID'],right_on=['END ID'])




    frameSI = pd.merge(frameSI,frameSI2[['END ID','Numero','Status', 'Data de Criacao','Alarme','count']], how='left',left_on=['END ID'],right_on=['END ID'])
    frameSI.rename(columns={'Status_x':'Status',
                          'Numero_x':'Numero',
                          'Data de Criacao_x':'Data de Criacao',
                          'Alarme_x':'Alarme',
                          'count_x':'count(TOTAL)',
                          'Status_y':'Status_Open',
                          'Numero_y':'Numero_Open',
                          'Data de Criacao_y':'Data de Criacao_Open',
                          'Alarme_y':'Alarme_Open',
                          'count_y':'count_Open'}, 
                 inplace=True)
    
    
 
    frameSI[["count(TOTAL)", "count_Open"]] = frameSI[["count(TOTAL)", "count_Open"]].apply(pd.to_numeric)
    frameSI = frameSI.sort_values(['count(TOTAL)','count_Open'], ascending = [False, False])
    frameSI.insert(len(frameSI.columns),'Periodo',periodo)
    frameSI = frameSI.reset_index(drop=True)

    frameOfensores = frameSI.copy()
    #KeepList = ['END ID','Numero','NE ID','count(TOTAL)','Periodo']
    KeepList = ['END ID','count(TOTAL)','Periodo']
    locationBase_top0 = list(frameOfensores.columns)
    DellList = list(set(locationBase_top0)^set(KeepList))
    frameOfensores = frameOfensores.drop(DellList,1)
    frameOfensores = pd.merge(frameOfensores,frameSISumTime, how='left',left_on=['END ID'],right_on=['END ID_Sum'])      
    frameOfensores = frameOfensores.sort_values(['ActiveTime_Sum'], ascending = [False])
    frameOfensores = frameOfensores.drop_duplicates()
    lista3 =[]
    for i in dataArchive2:
        if i not in lista3:
            lista3.append(i)
    datarange = ''
    datarange = '_'.join(str(e) for e in lista3)
    frameOfensores = frameOfensores.drop(['END ID_Sum'],1) 
    frameOfensores = frameOfensores.reset_index(drop=True)    
    csv_path2 = os.path.join(script_dir, 'export/SmartService_EVE/' + datarange + '_SmartService_EVE_Ofensores_End_ID'+'.csv')
    frameOfensores.to_csv(csv_path2,index=True,header=True,sep=';')



# sites count
    frameOfensoresSites = frameSI.copy()
    KeepList = ['NE ID','count(TOTAL)','Periodo']
    locationBase_top0 = list(frameOfensoresSites.columns)
    DellList = list(set(locationBase_top0)^set(KeepList))
    frameOfensoresSites = frameOfensoresSites.drop(DellList,1)
    frameOfensoresSites = pd.merge(frameOfensoresSites,frameSISite, how='left',left_on=['NE ID'],right_on=['NE ID_Sum'])      
    frameOfensoresSites = frameOfensoresSites.sort_values(['ActiveTime_Sum'], ascending = [False])
    frameOfensoresSites = frameOfensoresSites.drop_duplicates()
    lista3 =[]
    for i in dataArchive2:
        if i not in lista3:
            lista3.append(i)
    datarange = ''
    datarange = '_'.join(str(e) for e in lista3) 
    frameOfensoresSites = frameOfensoresSites.drop(['NE ID_Sum'],1) 
    frameOfensoresSites = frameOfensoresSites.reset_index(drop=True)  
    csv_path2 = os.path.join(script_dir, 'export/SmartService_EVE/' + datarange + '_SmartService_EVE_Ofensores_SITES'+'.csv')
    frameOfensoresSites.to_csv(csv_path2,index=True,header=True,sep=';')

    




    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=True,header=True,sep=';')
    




