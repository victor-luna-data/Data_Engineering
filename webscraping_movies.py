import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

URL =  'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
database = 'Movies.db'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=['Average Rank','Film','Year'])

response = requests.get(URL)
soup = BeautifulSoup(response.text, 'html.parser')

tables = soup.find_all('table')
rows = tables[0].find_all('tr')

count = 0
dictionary = {'Average Rank': [], 'Film': [], 'Year': []}

for row in rows:
    if count < 50:
        cells = row.find_all('td')
        if len(cells) != 0:
            dictionary['Average Rank'].append(cells[0].text)
            dictionary['Film'].append(cells[1].text)
            dictionary['Year'].append(cells[2].text)
            count += 1

df2 = pd.DataFrame(dictionary)
df = pd.concat([df, df2], ignore_index=True)
df.to_csv(csv_path)

conn = sqlite3.connect(database)
df.to_sql('Top_Movies', conn, if_exists='replace', index = False)

print(pd.read_sql("""
    SELECT * FROM Top_Movies LIMIT 10""", conn))
conn.close()