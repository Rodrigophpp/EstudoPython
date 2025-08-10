import requests
import pandas as pd
from sqlalchemy import create_engine,text

page = 1
per_page = 50
max_page = 10000000

servidor = 'LAPTOP-4A6UT0U9\\SQLEXPRESS'
banco = 'Ambev'
schema = 'extr'
tabela = 'Breweries'
driver = 'ODBC+Driver+17+for+SQL+Server'
conexao_banco = create_engine(f'mssql+pyodbc://{servidor}/{banco}?driver={driver}&trusted_connection=yes')

while page <= max_page:
    endpoint = f'https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}'
    resposta = requests.get(endpoint).json()
    if len(resposta)==0:
        break
    else:
        base = pd.DataFrame(resposta)
        base['Endpoint']=endpoint
        renomeia_colunas = base.rename(columns={'Id':'Id','name':'Name','brewery_type':'BreweryType',\
                                                'address_1':'Address1','address_2':'Address2','address_3':'Address3',\
                                                'city':'city','state_province':'StateProvince','postal_code':'PostalCode',\
                                                'country':'Country','longitude':'Longitude','latitude':'Latitude',\
                                                'phone':'Phone','website_url':'WebSiteUrl','state':'State','street':'Street'})

        with conexao_banco.begin() as conexao:
            conexao.execute(text(f'delete from {schema}.{tabela}\
                                   where Endpoint=\'{endpoint}\''))
            renomeia_colunas.to_sql(name=tabela,schema=schema,con=conexao, if_exists='append',index=False)
    page = page + 1


