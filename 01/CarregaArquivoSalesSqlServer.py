import pandas as pd
import os
from sqlalchemy import create_engine,text
import shutil as sh

servidor = 'LAPTOP-4A6UT0U9\\SQLEXPRESS'
banco='Vendas'
schema='extr'
tabela='Sales'
driver = 'ODBC+Driver+17+for+SQL+Server'
diretorio = 'C:\\Users\\rodri\\Desktop\\Rodrigo\\DSA\\Estudo\\01\\Bases\\Sales\\'
diretorio_carregados = os.path.join(diretorio,'Carregados')

conexao_banco = create_engine(f'mssql+pyodbc://{servidor}/{banco}?driver={driver}&trusted_connection=yes')

lista_diretorio = [arquivo for arquivo in os.listdir(diretorio) if arquivo.lower().endswith('.csv')]

if len(lista_diretorio) > 0:

    for arquivo in lista_diretorio:
        caminho_completo = os.path.join(diretorio,arquivo)
        print(f'Arquivo: {caminho_completo} sendo importado...')

        carrega_arquivo = pd.read_csv(caminho_completo,sep=',')
        seleciona_coluna = carrega_arquivo[['Txn_Date','Customer_Id','Quantity','Total_Sales']]
        renomeia_colunas = seleciona_coluna.rename(columns={'Txn_Date':'TxnDate','Customer_Id':'CustomerId','Quantity':'Quantity','Total_Sales':'TotalSales'})
        renomeia_colunas['CaminhoArquivo'] = caminho_completo

        with conexao_banco.begin() as conexao:
            conexao.execute(text(f'delete {schema}.{tabela}\
                               where CaminhoArquivo = \'{caminho_completo}\''))
            renomeia_colunas.to_sql(name=tabela,con=conexao,schema='extr',if_exists='append',index=False)

        with conexao_banco.begin() as conexao:
            carrega_logs = pd.read_sql(text(f'select count(*) linhas from {schema}.{tabela}\
                                              where CaminhoArquivo = \'{caminho_completo}\''),conexao_banco)

            logs={'Tabela':schema+'.'+tabela,'CaminhoArquivo':caminho_completo,'Linhas':int(carrega_logs.at[0,'linhas'])}
            alterando_formato_logs = pd.DataFrame([logs])
            alterando_formato_logs.to_sql(name='Logs',schema=schema,con=conexao,if_exists='append',index=False)

        sh.move(caminho_completo,diretorio_carregados)

    print('Arquivos carregados com sucesso!')
else:
    print('Não há arquivos no diretorio!')




