# Code for ETL operations on Country-GDP data
# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
cols=['Country','GDP_USD_millions']
db='World_Economies.db'
table='Countries_by_GDP'
csv_path='./Countries_by_GDP.csv'
log_file='./etl_project_log.txt'

def extract(url, cols):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    r=requests.get(url).text
    soup=BeautifulSoup(r,'html.parser')
    df=pd.DataFrame(columns=cols)
    tb=soup.find_all('tbody')
    rows=tb[2].find_all('tr')
    for row in rows:
        cell=row.find_all('td')
        if len(cell) != 0:
            if cell[0].find('a') is not None and '—' not in cell[2]:
                data_dic={'Country':cell[0].a.contents[0],'GDP_USD_millions':cell[2].contents[0]}
                df1=pd.DataFrame(data_dic, index=[0])
                df=pd.concat([df,df1],ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name,connection,if_exists='replace',index=False)

def run_query(query, connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query)
    query_output=pd.read_sql(query,connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    ts_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    ts=now.strftime(ts_format)
    with open(log_file,'a') as lf:
        lf.write(ts + ' : ' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process.')
df=extract(url,cols)
log_progress('Data extraction complete. Initiating Transformation process.')
df=transform(df)
log_progress('Data transformation complete. Initiating loading process.')
load_to_csv(df,csv_path)
log_progress('Data saved to CSV file.')
connection=sqlite3.connect(db)
log_progress('SQL Connection initiated.')
load_to_db(df,connection,table)
log_progress('Data loaded to Database as table. Running the query.')
query='select * from Countries_by_GDP where GDP_USD_billions >= 100'
run_query(query,connection)
log_progress('Process Complete.')
connection.close()