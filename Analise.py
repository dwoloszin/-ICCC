import ImportDF
import os
import sys

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'.csv')





def processArchive():
  MERGE = ImportDF.processArchive('\export')
  


  STATUS = MERGE['STATUS'].unique()

  for STATUS1 in STATUS:
    MERGE.loc[(MERGE['STATUS'] == STATUS1) &
              (MERGE['STATUS_MicroStrategy'].isna() | MERGE['STATUS_ALTAIA'].isna())&
              (MERGE['CS_STATUS_SI'] == 'In Service'),['Analise TIM']] = 'Alterar CS_STATUS para Deactivated ,alteracao de ID para 4G-, ou desativacao de celula'

    MERGE.loc[(MERGE['STATUS'] == STATUS1) &
              (~MERGE['STATUS_MicroStrategy'].isna() | ~MERGE['STATUS_ALTAIA'].isna())&
              (MERGE['CS_STATUS_SI'] == 'In Service'),['Analise TIM']] = 'N/A [Celula c/ Trafego]'

    MERGE.loc[(MERGE['STATUS'] == STATUS1) &
              (MERGE['STATUS_MicroStrategy'].isna() | MERGE['STATUS_ALTAIA'].isna())&
              (MERGE['CS_STATUS_SI'].isna()),['Analise TIM']] = 'N/A [Celula s/ trafego sem relacionamento SI]'
    
    MERGE.loc[(MERGE['STATUS'] == STATUS1) &
              (MERGE['STATUS_MicroStrategy'].isna() | MERGE['STATUS_ALTAIA'].isna())&
              (MERGE['CS_STATUS_SI'] == 'In Service')&
              (~MERGE['STATUS_MicroStrategy_Mobile'].isna()),['Analise TIM']] = 'Alterar CS_STATUS para Deactivated ,alteracao de ID para 4G-, se mobile <> de 4G- Deactivated no mobile tbm'

    MERGE.loc[(MERGE['STATUS'] == STATUS1) &
              (~MERGE['STATUS_MicroStrategy'].isna() | ~MERGE['STATUS_ALTAIA'].isna())&
              (MERGE['CS_STATUS_SI'] != 'In Service'),['Analise TIM']] = 'Alterar CS_STATUS para In Service [Celula c/ Trafego]'

    MERGE.loc[(MERGE['STATUS'] == STATUS1) &
              (~MERGE['SITE_RSVIVO'].isna() & (MERGE['CELLSECTOR'].str[:1] != 'T')),['Analise TIM']] = 'RS Cadastro obrigatorio so de 2600 demais freq nao necessario'



  





























  MERGE.to_csv(csv_path,index=False,header=True,sep=';')