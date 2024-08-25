import psycopg2
import os
import traceback
import logging
import pandas as pd


postgres_host = 'postgres'
postgres_database = os.environ.get('PROCESSING_DB')
postgres_user = os.environ.get('POSTGRES_USER')
postgres_password = os.environ.get('POSTGRES_PASSWORD')
postgres_port = '5432'
dest_folder = './data'


def create_connection():
    try:
        conn = psycopg2.connect(
            host = postgres_host,
            database = postgres_database,
            user = postgres_user,
            password = postgres_password,
            port = postgres_port
        )
        cur = conn.cursor()
        logging.info("La conexion a postgres es correcta")
        return cur,conn
    except Exception as e:
        traceback.print_exc()
        logging.error("No se pudo crear la conexion")

def write_to_postgres():

    """ Create Connection """
    cur,conn = create_connection()
    
    """
    Crea el dataframe y escribe en la tabla de Postgres si aún no existe
    """
    df = pd.read_csv(f'{dest_folder}/Churn_Modelling-1.csv')
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
            (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

    logging.info(f' {inserted_row_count} filas del archivo CSV insertadas en la tabla churn_modelling con éxito')

    conn.commit()
    cur.close()
    conn.close()
    

if __name__ == '__main__':
    write_to_postgres()
