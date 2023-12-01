import pandas as pd 
import sqlalchemy as sa
import configparser


config = configparser.ConfigParser()
config.read('.env')

from utils.helper import transform_data, send_to_postgresql
from utils.constants import datasets, table_names

['DB_CONN']
db_host = config['DB_CONN']['host']
db_user = config['DB_CONN']['user']
db_password = config['DB_CONN']['password']
db_database = config['DB_CONN']['database']


#--------- Read and transform files. Convert to list
result = list(transform_data(*datasets))


#--------- Process and view the transformed DataFrames
for idx, df in enumerate(result):
    print(f"DataFrame {idx + 1}:")
    print(df)

#--------- Export transformed files to postgresql using 'to_sql' function
send_to_postgresql(result, table_names, db_user, db_password, db_host, db_database)

print('Exported')
