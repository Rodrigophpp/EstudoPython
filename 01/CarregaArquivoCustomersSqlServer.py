import pandas as pd
import os
from sqlalchemy import create_engine,text
import shutil as sh

servidor = 'LAPTOP-4A6UT0U9\\SQLEXPRESS'
banco = 'Vendas'
schema = 'extr'
tabela = 'Customers'
driver =  'ODBC+Driver+17+for+SQL+Server'
caminho_origem = 'C:\\Users\\rodri\\Desktop\\Rodrigo\\DSA\\Estudo\\01\\Bases\\Customers\\'
caminho_carregado = os.path.join(caminho_origem, 'Carregados')

conexao_banco = create_engine(f'mssql+pyodbc://{servidor}/{banco}?driver={driver}&trusted_connection=yes')

lista_diretorio = [arquivo for arquivo in os.listdir(caminho_origem) if arquivo.lower().endswith('.csv')]
if len(lista_diretorio) > 0:
    for arquivo in lista_diretorio:
        caminho_completo = os.path.join(caminho_origem, arquivo)
        carrega_arquivo = pd.read_csv(caminho_completo,sep=',')
        seleciona_colunas = carrega_arquivo[['Customer_ID','First_Name','Last_Name','Gender']]
        renomeia_colunas = seleciona_colunas.rename(columns={'Customer_ID':'CustomerId','First_Name':'FirstName','Last_Name':'LastName'})
        renomeia_colunas['CaminhoArquivo'] = caminho_completo
        with conexao_banco.begin() as conexao:
            conexao.execute(text(f'delete {schema}.{tabela}\
                               where CaminhoArquivo = \'{caminho_completo}\''))
            renomeia_colunas.to_sql(name=tabela,con=conexao,if_exists='append',schema=schema,index=False)

        with conexao_banco.begin() as conexao:
            carrega_logs = pd.read_sql(text(f'select count(*) linhas from {schema}.{tabela}\
                                          where CaminhoArquivo = \'{caminho_completo}\''),conexao)
            cria_logs = {'Tabela':schema+'.'+tabela,'CaminhoArquivo':caminho_completo,'Linhas':int(carrega_logs.at[0,'linhas'])}
            tabela_logs = pd.DataFrame([cria_logs])
            tabela_logs.to_sql(name='Logs',con=conexao,if_exists='append',schema=schema,index=False)

        sh.move(caminho_completo,caminho_carregado)
else:
    print('Não há arquivos no diretorio!')


