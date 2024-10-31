import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import sqlite3
import numpy as np

def log_msg(title):
    with open('etl_project_log.txt', 'a') as f:
        f.write(title + ',' + datetime.now().strftime('%Y-%h-%d-%H:%M:%S') + '\n')

log_msg('Preliminaries complete. Initiating ETL process.')

URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'


response = requests.get(URL)
soup = BeautifulSoup(response.text, 'html.parser')


table = soup.find_all('table')[2]

rows = table.find_all('tr')



dict_df = {'Country':[], 'Estimate':[], 'Year':[]}
for row in rows:
    cells = row.find_all('td')
    if len(cells) != 0:
        dict_df['Country'].append(cells[0].text.strip())
        dict_df['Estimate'].append(cells[2].text)
        dict_df['Year'].append(cells[3].text)

df = pd.DataFrame(dict_df)
log_msg('Data extraction complete. Initiating Transformation process.')


df['Estimate'] = df['Estimate'].astype(str)
df['Estimate'].replace({',':'', '—': np.nan}, regex=True, inplace=True)
df.dropna(subset=['Estimate'], inplace=True)
df['Estimate'] = round(df['Estimate'].astype(int) / 1000,2)
df.rename({'Estimate':'GDP_USD_billions'}, axis=1, inplace= True)
log_msg('Data transformation complete. Initiating loading process.')


df.to_csv('Countries_by_GDP.csv')
log_msg('Data saved to CSV file.')


conn = sqlite3.connect('World_Economies.db')
log_msg('SQL Connection initiated.')
df.to_sql('Countries_by_GDP', con= conn, if_exists= 'replace')
log_msg('Data loaded to Database as table. Running the query.')


print(pd.read_sql("""
    SELECT * FROM Countries_by_GDP
        WHERE GDP_USD_billions >= 100
        ORDER BY GDP_USD_billions DESC
                     """, con= conn))
conn.close()
log_msg('Process Complete.')