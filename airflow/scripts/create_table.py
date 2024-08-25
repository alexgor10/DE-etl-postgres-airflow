import psycopg2
import os
import traceback
import logging


postgres_host = 'postgres'
postgres_database = os.environ.get('PROCESSING_DB')
postgres_user = os.environ.get('POSTGRES_USER')
postgres_password = os.environ.get('POSTGRES_PASSWORD')
postgres_port = '5432'


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

def create_postgres_table():
    """ Create Connection """
    cur,conn = create_connection()
    
    """
    Crea la tabla de Postgres con un esquema deseado
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")
        
        logging.info(' Nueva tabla churn_modelling creada exitosamente en el servidor de postgres')
    except:
        logging.warning(' Verifica si la tabla churn_modelling existe')

    conn.commit()
    cur.close()
    conn.close()
    

if __name__ == '__main__':
    create_postgres_table()
