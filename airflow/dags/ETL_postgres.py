from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


# Define Tasks
dag = DAG(
    'ETL',
    description='Extract data and save y postgres database',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

get_data_task = BashOperator(
    task_id='task_get_data',
    bash_command='python /opt/airflow/scripts/download_file.py',
    dag=dag
)

create_table_task = BashOperator(
    task_id='task_create_table',
    bash_command='python /opt/airflow/scripts/create_table.py',
    dag=dag
)

write_data_task = BashOperator(
    task_id='task_write_data',
    bash_command='python /opt/airflow/scripts/write_data.py',
    dag=dag
)

# Define task dependencies
get_data_task >> create_table_task >> write_data_task
