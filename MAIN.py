import ImportDF
import pandas as pd
import os
import sys
import ShortName
import MS
import ALTAIA_4G_IOT
import SmartService_EVE
import ALTAIA_2G
import ALTAIA_3G
import ALTAIA_4G
import ALTAIA_5G
import ALTAIA_4G_IOT
from UpdateWebArchives import updateArchives
import Analise
import DUMP_4G_VIVO_RS

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'.csv')

updateArchives()# atualiza os dados das rede

ALTAIA_2G.processArchive()
ALTAIA_3G.processArchive()
ALTAIA_4G.processArchive()
ALTAIA_5G.processArchive()
ALTAIA_4G_IOT.processArchive()
DUMP_4G_VIVO_RS.processArchive()



pathToImportBase = '\import\PBI_ICCC'
BASE_ANALISE = ImportDF.processArchive(pathToImportBase)
BASE_ANALISE.loc[BASE_ANALISE['MOBILESITE'].isna(),['MOBILESITE']] = BASE_ANALISE['SITE_NAME']
BASE_ANALISE = ShortName.tratarShortNumber(BASE_ANALISE,'MOBILESITE')


fields = ['MOBILE_SITE_NAME','CS_NAME','CS_STATUS']
pathToImportSI = '\import\SI'
SI = ImportDF.processArchive2(fields,pathToImportSI)
#SI = ShortName.tratarShortNumber(SI,'MOBILE_SITE_NAME')
SI.Name = 'SI'
SI = ImportDF.change_columnsName(SI)

fields2 = ['NAME','PROVISIONSTATUS','IMPLEMENTATIION_STATUS','ANF','MUNICIPIO']
pathToImportMO = '\import\Mobile'
MO = ImportDF.processArchive2(fields2,pathToImportMO)
#MO = ShortName.tratarShortNumber(MO,'NAME')
MO.Name = 'Mobile'
MO = ImportDF.change_columnsName(MO)


fieldsRSVIVO = ['SITE']
pathToImportRSVIVO = '\export\DUMP\RSVIVO'
RSVIVO = ImportDF.processArchive2(fieldsRSVIVO,pathToImportRSVIVO)
#MO = ShortName.tratarShortNumber(MO,'NAME')
RSVIVO.Name = 'RSVIVO'
RSVIVO = ImportDF.change_columnsName(RSVIVO)





MS_2 = MS.processArchive()
MS_2.Name = 'MicroStrategy'
MS_2 = ImportDF.change_columnsName(MS_2)



fieldsAltaia = ['SITE','CELL','STATUS','VOLUME']
pathToImportAltaia = '\export\ALTAIA'
ALTAIA = ImportDF.processArchive2(fieldsAltaia,pathToImportAltaia)
ALTAIA.Name = 'ALTAIA'
ALTAIA = ImportDF.change_columnsName(ALTAIA)



BULKLOAD_CELLSECTOR_Columns = ['MS_NAME','CELL_SECTOR_NAME','SI_PORT_NAME','PROVISION_STATUS']
BULKLOAD_CELLSECTOR = ImportDF.processArchive2(BULKLOAD_CELLSECTOR_Columns,'/import/'+ 'BulkLoad')
BULKLOAD_CELLSECTOR.Name = 'BULKLOAD_CELLSECTOR'
BULKLOAD_CELLSECTOR = ImportDF.change_columnsName(BULKLOAD_CELLSECTOR)


#ClaenBlackList
script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
csv_path = os.path.join(script_dir, 'import/BLACK_LIST/LastModification/'+'BLACK_LIST'+'.csv')
frameSI = ImportDF.processArchive('/import/BLACK_LIST/')
frameSI['DataAnalise'] = pd.to_datetime(frameSI['DataAnalise'], format="%d/%m/%Y")
frameSI.sort_values(by='DataAnalise', ascending=False, inplace=True)
subnetcheck = ['CellName']
frameSI.drop_duplicates(subset=subnetcheck, keep='first', inplace=True, ignore_index=False)
frameSI.to_csv(csv_path,index=False,header=True,encoding='ANSI',sep=';')




BLACKLIST_Columns = ['CellName','DataAnalise','Infoanalise']
BLACKLIST = ImportDF.processArchive2(BLACKLIST_Columns,'/import/BLACK_LIST/'+ 'LastModification')
BLACKLIST.Name = 'BLACKLIST'
BLACKLIST = ImportDF.change_columnsName(BLACKLIST)



frameSI = pd.merge(BASE_ANALISE,SI, how='left',left_on=['CELLSECTOR'],right_on=['CS_NAME_SI'])
frameSI = frameSI.drop(['CS_NAME_SI'], axis=1)

frameSI = pd.merge(frameSI,MO, how='left',left_on=['MOBILESITE'],right_on=['NAME_Mobile'])
frameSI = frameSI.drop(['NAME_Mobile'], axis=1)

frameSI = pd.merge(frameSI,MS_2, how='left',left_on=['CELLSECTOR'],right_on=['CELL_MicroStrategy'])
frameSI = frameSI.drop(['CELL_MicroStrategy'], axis=1)

frameSI = pd.merge(frameSI,ALTAIA, how='left',left_on=['CELLSECTOR'],right_on=['CELL_ALTAIA'])
frameSI = frameSI.drop(['CELL_ALTAIA'], axis=1)

frameSI = pd.merge(frameSI,BULKLOAD_CELLSECTOR, how='left',left_on=['CELLSECTOR'],right_on=['CELL_SECTOR_NAME_BULKLOAD_CELLSECTOR'])
frameSI = frameSI.drop(['CELL_SECTOR_NAME_BULKLOAD_CELLSECTOR'], axis=1)

frameSI = pd.merge(frameSI,BLACKLIST, how='left',left_on=['CELLSECTOR'],right_on=['CellName_BLACKLIST'])
frameSI = frameSI.drop(['CellName_BLACKLIST'], axis=1)

frameSI = pd.merge(frameSI,RSVIVO, how='left',left_on=['SITE_NAME'],right_on=['SITE_RSVIVO'])
#frameSI = frameSI.drop(['SITE_RSVIVO'], axis=1)



#Verificar se o novo 4G- tem trafrego
frameSI['MobileTemp4G'] = '4G-'+frameSI['SHORT'].astype(str)

MS2 = MS_2.copy()

KeepListCompared = ['SITE_MicroStrategy','STATUS_MicroStrategy']
locationBase_comparePMO = list(MS2.columns)
DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
MS2 = MS2.drop(DellListComparede,1)
MS2 = MS2.loc[MS2['STATUS_MicroStrategy'] == 'ATIVO']
MS2 = MS2.drop_duplicates()
MS2 = MS2.reset_index()
MS2.rename(columns={'SITE_MicroStrategy':'SITE_MicroStrategy2','STATUS_MicroStrategy':'STATUS_MicroStrategy_Mobile'},inplace=True)


frameSI = pd.merge(frameSI,MS2, how='left',left_on=['MobileTemp4G'],right_on=['SITE_MicroStrategy2'])
frameSI = frameSI.drop(['SITE_MicroStrategy2'], axis=1)

csv_path2 = os.path.join(script_dir, 'export/'+'MERGE'+'.csv')
frameSI.to_csv(csv_path2,index=False,header=True,encoding='ANSI',sep=';')

Analise.processArchive()



#MARS
MERGELIST_Columns = ['CELLSECTOR','LOCATION','Infoanalise_BLACKLIST','Analise TIM','STATUS']
MERGELIST = ImportDF.processArchive2(MERGELIST_Columns,'/export')
MERGELIST.Name = 'MERGELIST'
MERGELIST = ImportDF.change_columnsName(MERGELIST)

nbulkload_Columns = ['CELLSECTOR']
nbulkload = ImportDF.processArchive('/import/'+ 'nbulkload')

MERGELIST = pd.merge(MERGELIST,nbulkload, how='left',left_on=['CELLSECTOR_MERGELIST'],right_on=['CELLSECTOR'])
#MERGELIST = MERGELIST.drop(['CS_NAME_SI'], axis=1)

MERGELIST = MERGELIST.loc[~MERGELIST['CELLSECTOR'].isna()]


csv_path2 = os.path.join(script_dir, 'export/MARS/'+'MERGED'+'.csv')
MERGELIST.to_csv(csv_path2,index=False,encoding='ANSI',header=True,sep=';')








