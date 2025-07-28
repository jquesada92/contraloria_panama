import warnings
warnings.filterwarnings("ignore")



from pandas import read_excel, options, to_datetime,concat
options.mode.chained_assignment = None

from datetime import datetime as dt
from requests import post, get
from urllib.parse import quote
from bs4 import BeautifulSoup
from os  import makedirs, chdir, path
from re import search
import io
import os

print('Ingestion Process')

SOURCE_PATH =  'G:\My Drive\Colab Notebooks\contraloria'

def validate_last_update_date(func):
    print('Checking for updates')
    def wrapper(*args, **kwgars):

        def read_update_date_from_url():
            text = 'Fecha de actualización de los datos:\s+(\d{1,2}/\d{1,2}/\d+\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w+)'
            response = get('https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index',verify=False).text
            extract_date = search(text, response)
            return extract_date.group(1)


        file = 'last_updated_date.txt'
        webpage_date = dt.strptime(read_update_date_from_url() ,'%d/%m/%Y %H:%M:%S %p')
        try: 
            with open(file,'r+') as f:
                date = str(f.read())
                print(date)
                last_date = dt.strptime(date.strip().lstrip('\x00'),'%Y-%m-%d %H:%M:%S')

                if last_date < webpage_date:
                    func(*args, **kwgars)
                    f.truncate(0)
                    f.write(str(webpage_date).lstrip('\x00'))
                else:
                    print('no updates available')

        except FileNotFoundError:
            func(*args, **kwgars)
            with open(file,'w') as f:
                f.write(str(webpage_date))

    return wrapper


@validate_last_update_date
def execute_ingestion():
    print('starting update')
    errors = []
    def list_of_institutions():
        return list(filter(lambda x: x not in ('',"-- Seleccione una institución --"),
            BeautifulSoup(
                            post('https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index',verify=False).content, 'html.parser'
            )
                .find("select", {"id": "MainContent_ddlInstituciones"})
                    .text
                    .split('\n')
               ))

    def get_source_data(INSTITUCION:str):

        try:
            fecha_consulta = dt.now()
            url = f'https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Reporte?&Ne={quote(INSTITUCION)}&N=&A=&C=&E='
            file = get(url,verify=False)
            bytes_file_obj = io.BytesIO()
            bytes_file_obj.write(file.content)
            bytes_file_obj.seek(0)  # set file object to start
            def clean_data(data):
                df = data.iloc[4:].copy()
                df.columns = data.iloc[3]
                return df
            data = list(read_excel(bytes_file_obj,engine='openpyxl',sheet_name=None).values())
            informe = data[0].iloc[0,0]
            fecha_act = data[0].iloc[1,4]
            institucion  = data[0].iloc[1,0]
            df = concat(list(map(clean_data,data)))
            file_name = search('filename="(.*?).xlsx"',file.headers['content-disposition']).group(1)
            
            df['Salario'] = df['Salario'].astype('float')
            df['Gasto'] = df['Gasto'].astype('float')
            df['Fecha Actualizacion'] = str(
                dt.strptime(
                            search('.*?:\s+(\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w{2})',fecha_act).group(1)
                    ,'%d/%m/%Y %H:%M:%S %p')
            )
            df['Fecha Consulta'] = fecha_consulta
            df['archivo'] = file_name
            df['Institucion'] = INSTITUCION
            df.columns = [c.lower().replace(' ','_') for c in df.columns]
            df = df.rename(columns  = {'cédula':'cedula'})
            df['fecha_de_inicio'] = to_datetime(df['fecha_de_inicio'],format="%d/%m/%Y").dt.date
            df['fecha_actualizacion'] = to_datetime(df['fecha_actualizacion'])
            df['fecha_consulta'] = to_datetime(df['fecha_consulta'])
            df.to_parquet(f'''{SOURCE_PATH}/new/{file_name}+{str(fecha_consulta.timestamp()).replace('.','_')}.parquet''',
                          index=False,
                          use_deprecated_int96_timestamps=True)
        except Exception as e:
            print(INSTITUCION,url,e)

    def _run(lst_of_institutions:list):
        
        list(map(get_source_data,lst_of_institutions))

    _run(list_of_institutions())

    if len(errors)>0:
        _run(errors)







execute_ingestion()
