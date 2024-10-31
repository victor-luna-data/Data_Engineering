import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import sqlite3

def log_progress(msg, file_path):
    with open(file_path, 'a') as f:
        date = datetime.now().strftime('%Y-%h-%d %H:%M:%S')
        f.write(date + ',' + msg + '\n')

def extract(url, table_attribs):
    response = requests.get(url)
    data_dic = {table_attribs[0]: [], table_attribs[1]:[]}
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('table')[0]
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells)!=0:
            data_dic[table_attribs[0]].append(cells[1].text)
            data_dic[table_attribs[1]].append(cells[2].text)
    df = pd.DataFrame(data_dic)
    df.replace('\n', '',regex=True, inplace=True)
    df[table_attribs[1]] = df[table_attribs[1]].astype(float)
    return df
def transform(df, csv_path):
    currency_df = pd.read_csv(csv_path)
    dict = currency_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion']*dict['GBP'], 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion']*dict['EUR'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion']*dict['INR'], 2)
    return df
def load_to_csv(df, output_path):
    df.to_csv(output_path)
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace')
def run_query(query_statement, sql_connection):
    print(pd.read_sql(query_statement,sql_connection))


# Star Process

data_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
output_csv_path = 'Largest_banks_data.csv'
database_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

log_progress('Preliminaries complete. Initiating ETL process',log_file)

df = extract(data_url,['Name','MC_USD_Billion'])

log_progress('Data Extraction complete. Initiating Transformation process', log_file)

df = transform(df,exchange_rate_url)

log_progress('Data Transformation complete. Initiating Loading process',log_file)

load_to_csv(df, output_csv_path)

log_progress('Data saved to CSV file',log_file)

conn = sqlite3.connect(database_name)

log_progress('Data Connection initiated',log_file)

load_to_db(df,conn,table_name)

log_progress('Data loaded to Database as a table, Executing queries',log_file)

run_query('SELECT * FROM Largest_banks', conn)
run_query('SELECT  AVG(MC_GBP_Billion) FROM Largest_banks', conn)
run_query('SELECT Name FROM Largest_banks LIMIT 5', conn)

log_progress('Process Complete',log_file)

conn.close()

log_progress('Server Connection closed',log_file)