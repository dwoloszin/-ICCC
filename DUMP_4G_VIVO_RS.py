import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import ImportDF

def processArchive():
  fields = ['Operadora CEDENTE','Cell Name','PCI','TAC(DEC)','eNodebID(DEC)','ECGI(DEC)','UF','Operadora CEDENTE','Status','FreqInicialMHz','EnodeB Name','Sector']
  fields2 = ['BSC|RNC','CELL','PSC|PCI|NRPCI','TAC|LAC','CID|NBID|GNBID','BW DL','UF','Owner','STATUS','FREQ CELL','SITE','Setor']
  pathImport = '/import/DUMP_4G_VIVO'
  archiveName = pathImport[8:len(pathImport)]
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/DUMP/RSVIVO/'+archiveName+'RS.csv')

  frameSI = ImportDF.ImportDF_Xlsx(fields,fields2, pathImport,'RS 4G 2600 Dados Lógicos',1)
  #frameSI = ImportDF.ImportDF_csv(fields,fields2,pathImport)
 
  frameSI = frameSI.loc[(frameSI['Owner'].astype(str) == 'VIVO') & (frameSI['UF'].astype(str) == 'SP') ]
  frameSI['CELL'] = frameSI['SITE'].astype(str) + frameSI['Setor'].astype(str)
  frameSI['BW DL'] = ''
  frameSI = frameSI.loc[frameSI['FREQ CELL'] != 'iot']
  frameSI['Tec'] = '4G'

  #frameSI['FREQ CELL'] = frameSI['UARFCN DL'].map({4379:'900',10787:'2100',10811:'2100',10832:'2100'})
 
  frameSI = frameSI.drop_duplicates()
  frameSI = frameSI.reset_index(drop=True)
  frameSI.to_csv(csv_path,index=True,header=True,sep=';')
 

