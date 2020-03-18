#!/usr/bin/env python
# coding: utf-8

# In[56]:


import getpass
import cx_Oracle
import datetime
import time
from IPython.display import HTML, display
from IPython.display import clear_output
import tabulate
import os
import jpype
import jaydebeapi
import pandas as pd
from sqlalchemy import create_engine
import time
import csv
import glob
from pathlib import Path
import json

### Configuração da Extração no Oracle ###
db_hive = ''
#db_hive = ''
ora_tables = ['','']
#path = 'D:\\anotacoes\\csm\\'
path = 'R:\\csm\\'
database = ''
user=''
senha=''
token=[user, senha]


# In[60]:


def executa_query(query):
    cursor = connection.cursor()
    result = cursor.execute(query)
    col_names = tuple([row[0] for row in cursor.description])
    rows = cursor.fetchall()
    rows.insert(0, col_names)    
    display(HTML(tabulate.tabulate(rows, tablefmt='html')))
    
#https://help.sap.com/saphelp_repserver1571300/helpdata/en/d8/e22476e7b14b88b182ca5f936e12f2/frameset.htm
#https://docs.oracle.com/goldengate/v12212/gg-veridata/GVDUG/column-mapping.htm#GVDUG-GUID-61924286-7CDB-4954-B8F1-6D9CD6170851
#'long','clob','nclob' entram como STRING', não são necessários colocar no dicionário.
def switch(tipo):
    switcher = {
        'integer': 'DECIMAL',
        'number': 'DECIMAL',
        'number(10)':'BIGINT',
        'number(11)':'BIGINT',
        'number(12)':'BIGINT',
        'number(13)':'BIGINT',
        'number(14)':'BIGINT',
        'number(15)':'BIGINT',
        'number(16)':'BIGINT',
        'number(17)':'BIGINT',
        'number(18)':'BIGINT',
        'number(5)':'INT',
        'number(6)':'INT',
        'number(7)':'INT',
        'number(8)':'INT',
        'number(9)':'INT',
        'number(1)':'TINYINT',
        'float':'DECIMAL',
        'binary_float':'DECIMAL',
        'binary_double':'DECIMAL',
        'date':'TIMESTAMP',
        'char':'VARCHAR',
        'nchar':'VARCHAR',
        'varchar2':'VARCHAR',
        'nvarchar2':'VARCHAR',
        'raw':'BINARY',
        'longraw':'BINARY',
        'blob':'BINARY',
        'timestamp':'TIMESTAMP'       
    }
    return switcher.get(tipo,"STRING")


# In[15]:


for ora_table in ora_tables:
    print(f"Escrevendo csv: {ora_table}")
    # CONECTANDO VIA SCAN LISTENER -- SANDBOX
    connection = cx_Oracle.connect(token[0], token[1], database, encoding="UTF-8")
    cursor = connection.cursor()
    sql=f'SELECT * FROM UR_EMPRESAS.{ora_table} ORDER BY CNPJ'
    cursor.execute(sql)
    
    # ESCREVENDO ARQUIVO CSV
    start_time = time.time()

    #cursor.arraysize = 256
    column_names = [i[0] for i in cursor.description]
    with open(path + f'{ora_table}.csv', 'w', encoding='utf-8') as csvfile:
        wr = csv.writer(csvfile, lineterminator = '\n', delimiter=';')
        wr.writerow(column_names)
        for row in cursor:
            wr.writerow(row)

    elapsed_time = time.time() - start_time
    print(elapsed_time)

    
    


# In[61]:


# CSV to JSON
# Gera um único JSON para todos os arquivos csv que estão no path, cada arquivo csv é uma tabela

# Parâmetros gerais
db_name = f"{db_hive}"
csv_col_sep = ";"
delimit_dec = "."

######################################
# Gerando o JSON com base nos CSVs   #
######################################

print("Gerando JSON descritor dos metadados do CSV...")

# DDL de criação das tabelas para o dialeto escolhido.
# É usado o dialeto MySQL aqui por ser muito similar ao utilizado pelo Hive.
# Preenchimento de um dict para traduzir os dados do esquema para o formato JSON esperado pela ingestão no DataLake

json_dict = {}

# A base será considerada como do tipo arquivo
json_dict["nome"] = db_name
json_dict["tipo"] = "arquivo"
json_dict["comentario"] = ""
json_dict["owner"] = ""
json_dict["delimitadores"] = { "colunas": csv_col_sep, "linhas": "\\n" }
json_dict["tabelas"] = []

os.chdir(path)
csv_files = None
csv_files = glob.glob('*.csv')
# Tirando extensão csv do arquivo
tables = []
tables = list(map(lambda x: (x[:-4]), csv_files))

####################
# Conteúdo do JSON #
####################


for file in csv_files:
    df = pd.read_csv(path + file, nrows=0, header=0, sep=csv_col_sep)

    tab_dict = {}
    # Tirando extensão csv
    tab_dict["nome"] = file[:-4]
    tab_dict["comentario"] = ""
    tab_dict["colunas"] = []
    
    #Criando dicionário dos tipos de dados encontrados no Oracle
    sql_dict=f"SELECT column_name,data_type,data_length FROM all_tab_columns WHERE table_name='{file[:-4]}'"
    cursor.execute(sql_dict)
    df_pd_dict = pd.read_sql(sql_dict, connection)
    df_pd_dict.insert(2, 'comentarios', '')

    print(f'Dicionário {file[:-4]}: {df_pd_dict}')


    for column in df.columns:
        print(f"Escrevendo: {column}/{file[:-4]}")
        col_dict = {}
        col_dict["nome"] = column
        for variavel in df_pd_dict.iloc[:,0]:
            # verificando tipo de dado Hive conforme dicionário
            tipo = switch(df_pd_dict[df_pd_dict.iloc[:,0]==variavel].iloc[0,1].lower())
            print(f'Tipo: {tipo}')
            if column == variavel:
                if (tipo == 'DECIMAL' or tipo == 'DOUBLE' or tipo == 'FLOAT'): 
                    print(column)
                    precisao = 2
                    col_dict["tipo"] = tipo
                    col_dict["comentario"] = df_pd_dict[df_pd_dict.iloc[:,0]==variavel].iloc[0,2]
                    col_dict["tamanho"] = int(df_pd_dict[df_pd_dict.iloc[:,0]==variavel].iloc[0,3])
                    col_dict["precisao"] = precisao
                    break
                if(tipo == 'STRING'):
                    col_dict["tipo"] = "STRING"
                    col_dict["comentario"] = ""
                    col_dict["tamanho"] = "0"
                    col_dict["precisao"] = "0"   
                else:
                    print(column)
                    precisao = 2
                    col_dict["tipo"] = tipo
                    col_dict["comentario"] = df_pd_dict[df_pd_dict.iloc[:,0]==variavel].iloc[0,2]
                    col_dict["tamanho"] = int(df_pd_dict[df_pd_dict.iloc[:,0]==variavel].iloc[0,3])
                    col_dict["precisao"] = 0
                    break
            else:
                col_dict["tipo"] = "STRING"
                col_dict["comentario"] = ""
                col_dict["tamanho"] = "0"
                col_dict["precisao"] = "0"

        tab_dict["colunas"].append(col_dict)

    json_dict["tabelas"].append(tab_dict)


# In[62]:


# Grava arquivo JSON no diretório
try:
    with open(path + f"{db_name}_meta.json", "w") as arq:
        json.dump(json_dict, arq, indent=2)
except Exception as exc:
    print("Não foi possível gravar o arquivo json")
    print(str(exc))
    exit(1)    


# In[ ]:


##### FIM #####


# In[ ]:




