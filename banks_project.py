# Code for ETL operations on largest banks data
# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
url='https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
rate_csv='/home/project/exchange_rate.csv'
db='Banks.db'
table='Largest_banks'
table_columns=['Name','MC_USD_Billion']
output_file='./Largest_banks_data.csv'
log_file='./code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    ts_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    ts=now.strftime(ts_format)
    with open(log_file,'a') as lf:
        lf.write(ts + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df=pd.DataFrame(columns=table_attribs)
    page=requests.get(url).text
    soup=BeautifulSoup(page,'html.parser')
    tables=soup.find_all('tbody')
    rows=tables[0].find_all('tr')
    for row in rows:
        cell=row.find_all('td')
        if len(cell)!=0:
            t_dict={'Name':cell[1].contents[2],'MC_USD_Billion':float(cell[2].contents[0])}
            df1 = pd.DataFrame(t_dict,index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df
df=extract(url,table_columns)
log_progress('Data extraction complete. Initiating Transformation process')
print(df)

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    rate_df=pd.read_csv(csv_path)
    rate_dict=rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion']=[np.round(x*rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion']=[np.round(x*rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion']=[np.round(x*rate_dict['INR'],2) for x in df['MC_USD_Billion']]
    return df
df=transform(df,rate_csv)
log_progress('Data transformation complete. Initiating Loading process')
print(df)

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)
load_to_csv(df,output_file)
log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)
connection=sqlite3.connect(db)
log_progress('SQL Connection initiated')
load_to_db(df,connection,table)
log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output=pd.read_sql(query_statement,sql_connection)
    print(query_output)
query1='SELECT * FROM Largest_banks'
query2='SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3='SELECT Name from Largest_banks LIMIT 5'
run_query(query1,connection)
run_query(query2,connection)
run_query(query3,connection)
log_progress('Process Complete')
connection.close()
log_progress('Server Connection closed')