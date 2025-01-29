from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import pendulum
import sys

# Добавьте путь к папке с transform_script.py
sys.path.insert(0, os.path.abspath("/home/dzharlaksl/Documents/URFU/3 семестр/big_data_itog"))

from transform_script import transfrom

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 5, tzinfo=pendulum.timezone('UTC')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract(**kwargs):
    # Чтение данных из CSV
    profit_table = pd.read_csv('/opt/airflow/data/profit_table.csv')
    return profit_table.to_json()

def transform(**kwargs):
    ti = kwargs['ti']
    profit_table_json = ti.xcom_pull(task_ids='extract')
    profit_table = pd.read_json(profit_table_json)
    
    # Определение даты расчета (конец предыдущего месяца)
    execution_date = kwargs['execution_date']
    calculation_date = execution_date.replace(day=1) - timedelta(days=1)
    calculation_date_str = calculation_date.strftime('%Y-%m-01')
    
    # Вызов функции трансформации
    flags_df = transfrom(profit_table, calculation_date_str)
    
    # Добавление колонки с датой расчета
    flags_df['calculation_date'] = calculation_date_str
    return flags_df.to_json()

def load(**kwargs):
    ti = kwargs['ti']
    flags_json = ti.xcom_pull(task_ids='transform')
    flags_df = pd.read_json(flags_json)
    
    file_path = '/opt/airflow/data/flags_activity.csv'
    
    # Сохранение с добавлением новых данных
    write_header = not os.path.exists(file_path)
    flags_df.to_csv(file_path, mode='a', index=False, header=write_header)

with DAG(
    'flags_activity_dag',
    default_args=default_args,
    schedule_interval='0 0 5 * *',  # 5-го числа каждого месяца
    catchup=False,
    tags=['activity_flags'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task