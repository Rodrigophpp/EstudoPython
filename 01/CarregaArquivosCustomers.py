import pandas as pd
from sqlalchemy import create_engine,text
import os
import shutil as sh


pasta_arquivos = 'C:\\Users\\rodri\\Desktop\\Rodrigo\\DSA\\Estudo\\01\\Bases\\Customers\\'
pasta_carregados = os.path.join(pasta_arquivos, 'Carregados')

servidor = 'localhost'
porta = '5782'
banco = 'Vendas'
usuario = 'Postgres'
senha = 'Postgres'

conexao_banco = create_engine(f'postgresql+psycopg2://{usuario}:{senha}@{servidor}:{porta}/{banco}')

lista_diretorio = [arquivo for arquivo in os.listdir(pasta_arquivos) if arquivo.lower().endswith('.csv')]
if len(lista_diretorio) > 0:
    with conexao_banco.begin() as conexao:
        conexao.execute(text('truncate table extr.customers'))

    for arquivo in lista_diretorio:
        arquivo_completo = os.path.join(pasta_arquivos, arquivo)
        carrega_arquivo = pd.read_csv(arquivo_completo,sep=',')
        colunas_arquivo = carrega_arquivo[['Customer_ID', 'First_Name', 'Last_Name', 'Gender']]
        renomeia_colunas = colunas_arquivo.rename(columns={'Customer_ID':'CustomerID','First_Name':'FirstName','Last_Name':'LastName','Gender':'Gender'})
        with conexao_banco.begin() as conexao:
            renomeia_colunas.to_sql(name='customers',con=conexao,schema='extr',if_exists='append',index=False)

        sh.move(arquivo_completo, pasta_carregados)
else:
    print('Não há arquivos na pasta')









