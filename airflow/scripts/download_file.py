import os
import urllib.request
import traceback
import logging

url = 'https://raw.githubusercontent.com/xploiterx/datasets/master/Proyect-0/CSV/Churn_Modelling-1.csv'
dest_folder = '/opt/airflow/data'
destination_path = f'{dest_folder}/Churn_Modelling-1.csv'

def download_file_from_url(url: str, dest: str):
    if not os.path.exists(str(dest)):
        os.makedirs(str(dest))

    try:
        urllib.request.urlretrieve(url,destination_path)
        logging.info('Archivo descargado exitosamente al directorio de trabajo')
    except Exception as e:
        logging.error(f'Error al descargar el archivo CSV debido a: {e}')
        traceback.print_exc()


if __name__ == '__main__':
    download_file_from_url(url, dest_folder)