import pandas as pd
from sqlalchemy import create_engine,text
import os
import shutil as sh

servidor = 'localhost'
porta = '5782'
banco = 'Vendas'
usuario = 'Postgres'
senha = 'Postgres'

caminho_arquivo = 'C:\\Users\\rodri\\Desktop\\Rodrigo\\DSA\\Estudo\\01\\Bases\\Sales\\'
caminho_carregado = os.path.join(caminho_arquivo, 'Carregados')

conexao_banco = create_engine(f'postgresql+psycopg2://{usuario}:{senha}@{servidor}:{porta}/{banco}')

lista_arquivos = [arquivo for arquivo in os.listdir(caminho_arquivo) if arquivo.lower().endswith('.csv')]
if len(lista_arquivos) > 0:
    with conexao_banco.begin() as conexao:
        conexao.execute(text('truncate table extr.sales'))

    for arquivo in lista_arquivos:
        caminho_completo = os.path.join(caminho_arquivo, arquivo)
        carrega_arquivo = pd.read_csv(caminho_completo,sep=',')
        seleciona_coluna = carrega_arquivo[['Txn_Date','Customer_Id','Quantity','Total_Sales']]
        renomeia_arquivo = seleciona_coluna.rename(columns={'Txn_Date':'TxnDate','Customer_Id':'CustomerId','Quantity':'Quantity','Total_Sales':'TotalSales'})
        with conexao_banco.begin() as conexao:
            renomeia_arquivo.to_sql(name='sales',con=conexao,schema='extr',if_exists='append',index=False)
        sh.move(caminho_completo,caminho_carregado)
else:
    print('Não há arquivo na rede')



