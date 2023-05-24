import ImportDF
import os
import sys

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'.csv')





def processArchive():
  MERGE = ImportDF.processArchive('\export')
  


  Ref1 = 'CS_ATIVO_SEM_TRAFEGO (STATUS)'
  MERGE.loc[(MERGE['REF'] == Ref1) &
            (MERGE['STATUS_MicroStrategy'].isna() | MERGE['STATUS_IoT'].isna())&
            (MERGE['CS_STATUS_SI'] == 'In Service'),['Analise TIM']] = 'Alterar CS_STATUS para Deactivated ,alteracao de ID para 4G-, ou desativacao de celula'

  MERGE.loc[(MERGE['REF'] == Ref1) &
            (~MERGE['STATUS_MicroStrategy'].isna() | ~MERGE['STATUS_IoT'].isna())&
            (MERGE['CS_STATUS_SI'] == 'In Service'),['Analise TIM']] = 'N/A [Celula c/ Trafego]'

  MERGE.loc[(MERGE['REF'] == Ref1) &
            (MERGE['STATUS_MicroStrategy'].isna() | MERGE['STATUS_IoT'].isna())&
            (MERGE['CS_STATUS_SI'].isna()),['Analise TIM']] = 'N/A [Celula s/ trafego sem relacionamento SI]'
  

  




























  MERGE.to_csv(csv_path,index=False,header=True,sep=';')