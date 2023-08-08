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

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'.csv')

updateArchives()

ALTAIA_2G.processArchive()
ALTAIA_3G.processArchive()
ALTAIA_4G.processArchive()
ALTAIA_5G.processArchive()
ALTAIA_4G_IOT.processArchive()



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

fields2 = ['NAME','PROVISIONSTATUS','IMPLEMENTATIION_STATUS']
pathToImportMO = '\import\Mobile'
MO = ImportDF.processArchive2(fields2,pathToImportMO)
#MO = ShortName.tratarShortNumber(MO,'NAME')
MO.Name = 'Mobile'
MO = ImportDF.change_columnsName(MO)


MS = MS.processArchive()
MS.Name = 'MicroStrategy'
MS = ImportDF.change_columnsName(MS)



fieldsAltaia = ['SITE','CELL','STATUS']
pathToImportAltaia = '\export\ALTAIA'
ALTAIA = ImportDF.processArchive2(fieldsAltaia,pathToImportAltaia)
ALTAIA.Name = 'ALTAIA'
ALTAIA = ImportDF.change_columnsName(ALTAIA)









BULKLOAD_CELLSECTOR_Columns = ['MS_NAME','CELL_SECTOR_NAME','SI_PORT_NAME','PROVISION_STATUS']
BULKLOAD_CELLSECTOR = ImportDF.processArchive2(BULKLOAD_CELLSECTOR_Columns,'/import/'+ 'BulkLoad')
BULKLOAD_CELLSECTOR.Name = 'BULKLOAD_CELLSECTOR'
BULKLOAD_CELLSECTOR = ImportDF.change_columnsName(BULKLOAD_CELLSECTOR)





frameSI = pd.merge(BASE_ANALISE,SI, how='left',left_on=['CELLSECTOR'],right_on=['CS_NAME_SI'])
frameSI = frameSI.drop(['CS_NAME_SI'], axis=1)

frameSI = pd.merge(frameSI,MO, how='left',left_on=['MOBILESITE'],right_on=['NAME_Mobile'])
frameSI = frameSI.drop(['NAME_Mobile'], axis=1)

frameSI = pd.merge(frameSI,MS, how='left',left_on=['CELLSECTOR'],right_on=['CELL_MicroStrategy'])
frameSI = frameSI.drop(['CELL_MicroStrategy'], axis=1)

frameSI = pd.merge(frameSI,ALTAIA, how='left',left_on=['CELLSECTOR'],right_on=['CELL_ALTAIA'])
frameSI = frameSI.drop(['CELL_ALTAIA'], axis=1)

frameSI = pd.merge(frameSI,BULKLOAD_CELLSECTOR, how='left',left_on=['CELLSECTOR'],right_on=['CELL_SECTOR_NAME_BULKLOAD_CELLSECTOR'])
frameSI = frameSI.drop(['CELL_SECTOR_NAME_BULKLOAD_CELLSECTOR'], axis=1)

#Verificar se o novo 4G- tem trafrego
frameSI['MobileTemp4G'] = '4G-'+frameSI['SHORT'].astype(str)

MS2 = MS.copy()

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


frameSI.to_csv(csv_path,index=False,header=True,sep=';')




