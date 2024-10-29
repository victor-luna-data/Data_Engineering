import requests
import zipfile
import io
import pandas as pd
import glob
from datetime import datetime


def extraction(URL):
    response = requests.get(URL)
    # Descargar y extraer archivo zip
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall('C:/Users/Arkon/Documents/My_Projects/python/Data_Eng')
        
        print('Descargado y extraido')
    else:
        print('Error, estatus: ', response.status_code)

def from_csv(file):
    df = pd.read_csv(file)
    return df

def from_json(file):
    df = pd.read_json(file,lines=True)
    return df

def from_xml(file):
    df = pd.read_xml(file)
    return df

def extract_files():
    df = pd.DataFrame(columns=['name','height','weight'])

    for csv in glob.glob('Data_Eng/*.csv'): # glob busca archivos y directorios
        df = pd.concat([df, pd.DataFrame(from_csv(csv))], ignore_index= True)

    for json in glob.glob('Data_Eng/*.json'):
        df = pd.concat([df, pd.DataFrame(from_json(json))], ignore_index=True)

    for xml in glob.glob('Data_Eng/*.xml'):
        df = pd.concat([df, pd.DataFrame(from_xml(xml))], ignore_index=True)
    print('files concatenados')
    return df

def transform(df): # Convert inches to meters
    df['height'] = round(df['height'] * 0.0254, 2)

    df['weight'] = round(df['weight'] * 0.454, 2)

    return df

def load_data(target_file, df):
    df.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp + ',' + message + '\n')


# Flujo ETL        

URL = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip'
log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

log_progress('ETL Job Started')

log_progress('Download phase Started')
extraction(URL)
log_progress('Download phase Ended')

log_progress('Download phase Started')
df = extract_files()
log_progress('Download phase Ended')

log_progress('Transform phase Started')
df = transform(df)
log_progress('Transform phase Ended')

log_progress('Load phase Started')
load_data(target_file,df)
log_progress('Load phase Ended')

log_progress('ETL Job Ended')