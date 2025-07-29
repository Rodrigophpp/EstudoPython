import requests
import pandas as pd
from sqlalchemy import create_engine,text

usuario = 'Postgres'
senha = 'Postgres'
host = 'localhost'
porta = '5785'
banco = 'Ambev'
schema = 'extr'

page = 1
page_max = 10000
per_page = 50
nome_arquivo = 'cervejaria'

conexao_banco = create_engine(f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}')
with conexao_banco.begin() as conexao:
    conexao.execute(text(f'truncate table {schema}.{nome_arquivo}'))

while page <= page_max:
    endpoint = f'https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}'
    resposta = requests.get(endpoint).json()
    if len(resposta) == 0:
        break
    else:
        tabela = pd.DataFrame(resposta)
        tabela['endpoint'] = endpoint
        with conexao_banco.begin() as conexao:
            tabela.to_sql(name=nome_arquivo, schema=schema,con=conexao, if_exists='append', index=False)
        page = page + 1

with conexao_banco.begin() as conexao:
    total_linhas = pd.read_sql(text(f'select count (*) as total_cervejarias from {schema}.{nome_arquivo}'),conexao)
print(total_linhas)

