import json
import pandas as pd
import matplotlib.pyplot as plt
import pydoop.hdfs as hdfs
from datetime import datetime
from pandasql import sqldf
import os


login=''
senha=''

os.system('echo '+senha+' | kinit '+login)
dir = '/ranger/audit/hiveServer2/'
list = hdfs.ls(dir)

df = pd.DataFrame()
for pasta in list:
    for i in range(len(hdfs.ls(pasta))):
        try:
            with hdfs.open(hdfs.ls(pasta)[i], 'r') as f:
                jsn = [json.loads(line) for line in f]
                df = df.append([pd.DataFrame(jsn)], sort=True)
            
        except:
                print("Leitura do arquivo json em " + hdfs.ls(pasta)[i] + " não foi bem sucedida")

df1 = df[['evtTime','reqUser','resource','access','reqData']]
df1['reqUser'] = df1['reqUser'].str.upper()
df1 = df1[df1['access']=='SELECT']
# exclusao de usuarios de servico
exclusao = pd.DataFrame(['HIVE','RANGERLOOKUP'])
df1 = df1[~df1.reqUser.isin(exclusao.iloc[:,0])]
df1['evtTime'] = pd.to_datetime(df1['evtTime'].str[0:16], format='%Y-%m-%d %H:%M')

spark_df = spark.createDataFrame(df1)
spark_df.registerTempTable("df3")

select reqUser usuarios, count(1) acessos
from df3
where from_unixtime(unix_timestamp(evtTime)) > "${data_inicio=2019-06-01}" AND from_unixtime(unix_timestamp(evtTime)) < "${data_fim=2019-07-01}"
group by reqUser
order by acessos desc

select from_unixtime(unix_timestamp(evtTime)), count(*)
from df3
where from_unixtime(unix_timestamp(evtTime)) > "${data_inicio=2019-06-01}" AND from_unixtime(unix_timestamp(evtTime)) < "${data_fim=2019-08-30}"
group by from_unixtime(unix_timestamp(evtTime))
order by from_unixtime(unix_timestamp(evtTime)) asc

select from_unixtime(unix_timestamp(evtTime)),reqUser,resource,access
from df3
where from_unixtime(unix_timestamp(evtTime)) > "${data_inicio=2019-06-01}" AND from_unixtime(unix_timestamp(evtTime)) < "${data_fim=2019-08-30}" AND reqUser = "" 
order by from_unixtime(unix_timestamp(evtTime)) desc