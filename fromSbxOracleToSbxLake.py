#!/usr/bin/env python
# coding: utf-8

# In[223]:


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

db_hive = ''
ora_table = ''
usuario = ''


# In[224]:


user=''
print ('Usuário '+user) ; print('Entre com a sua senha:')
#senha = getpass.getpass()  ; token=[user, senha]
senha=''
token=[user, senha]


# In[225]:


#print('Digite o Database:'); database = input(); print(database)
database=''


# In[226]:


# CONECTANDO VIA SCAN LISTENER -- SANDBOX
connection = cx_Oracle.connect(token[0], token[1], database, encoding="UTF-8")
cursor = connection.cursor()


# In[227]:


sql=f'SELECT * FROM {usuario}.{ora_table} WHERE ROWNUM <= 10'
#executa_query(sql)


# In[228]:


#df = pd.read_sql(sql, connection)
cursor.execute(sql)

    
    


# In[229]:


# Para o JPype levantar a JVM correta
jre_path = 'c:\\Program Files (x86)\\Common Files\Oracle\Java\javapath\java'

os.environ['JAVA_HOME'] = jre_path


# In[230]:


# A autentição ao Hive (Data Lake) é baseada em Kerberos, então é necessário ter um ticket obtido com a mesma senha utilizada para se logar no computador
userHive = 'fecol'
passwordHive = ''
os.system(jre_path + "\\bin\\kinit " + userHive + " " + passwordHive)


# In[231]:


# Configurando a conexão e ambiente Java para funcionar com o Kerberos
# descaracterizacao dos dados da empresa por seguranca
driver = "org.apache.hive.jdbc.HiveDriver"
jar = "D:\\driver\\hive-jdbc-uber-2.6.5.0-292.jar"
url = "jdbc:hive2://,;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/"


# In[ ]:


args = '-Djava.class.path=' + jar
jvm_path = jpype.getDefaultJVMPath()
jpype.startJVM(jvm_path, args, '-Djavax.security.auth.useSubjectCredsOnly=false')


# In[232]:


# Realizando a conexão
conn_lake = jaydebeapi.connect(driver, url, {}, jar, )
cursor_hive = conn_lake.cursor()


# In[49]:


print(df)


# In[203]:


# Criando tabela vazia

cursor_hive.execute(f'create table {db_hive}.{ora_table}(ORIGEM_IMPORTACAO STRING not null,CNPJ_DEVEDOR STRING not null,COD_CREDOR STRING not null,EVENTO_COD STRING not null,EVENTO_DATA STRING not null)')


# In[85]:


#tuples = str([tuple(x) for x in df.values])[1:-1]
#parameters = [df.iloc[line, :].to_dict() for line in range(len(df))]
#parameters = df.to_dict(orient='records')
#lista = df.values.tolist()


# In[84]:


#cursor.execute(f"INSERT INTO TABLE {db_hive}.{ora_table} VALUES " + tuples)


# In[179]:





# In[204]:


start_time = time.time()

for row in cursor:
    cursor_hive.execute(f"INSERT INTO TABLE {db_hive}.{ora_table} VALUES " + str(row))

elapsed_time = time.time() - start_time
print(elapsed_time)
    


# In[233]:


cursor_hive.execute(f'create table {db_hive}.{ora_table}_arraysize(ORIGEM_IMPORTACAO STRING not null,CNPJ_DEVEDOR STRING not null,COD_CREDOR STRING not null,EVENTO_COD STRING not null,EVENTO_DATA STRING not null)')


# In[234]:



start_time = time.time()

cursor.arraysize = 256
for row in cursor:
    cursor_hive.execute(f"INSERT INTO TABLE {db_hive}.{ora_table}_arraysize VALUES " + str(row))

elapsed_time = time.time() - start_time
print(elapsed_time)


# In[235]:


cursor_hive.execute(f'create table {db_hive}.{ora_table}_full(ORIGEM_IMPORTACAO STRING not null,CNPJ_DEVEDOR STRING not null,COD_CREDOR STRING not null,EVENTO_COD STRING not null,EVENTO_DATA STRING not null)')

sql=f'SELECT * FROM UR_EMPRESAS.{ora_table}'

cursor.execute(sql)

cursor.arraysize = 256
for row in cursor:
    print(row)
    cursor_hive.execute(f"INSERT INTO TABLE {db_hive}.{ora_table}_full VALUES " + str(row))

elapsed_time = time.time() - start_time
print(elapsed_time)




