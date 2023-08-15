
import ImportDF
import os
import sys
import pandas as pd


script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
csv_path = os.path.join(script_dir, 'import/BLACK_LIST/LastModification/'+'BLACK_LIST'+'.csv')
frameSI = ImportDF.processArchive('/import/BLACK_LIST/')
frameSI['DataAnalise'] = pd.to_datetime(frameSI['DataAnalise'], format="%d/%m/%Y")
frameSI.sort_values(by='DataAnalise', ascending=False, inplace=True)
subnetcheck = ['CellName']
frameSI.drop_duplicates(subset=subnetcheck, keep='first', inplace=True, ignore_index=False)
frameSI.to_csv(csv_path,index=False,header=True,encoding='ANSI',sep=';')


