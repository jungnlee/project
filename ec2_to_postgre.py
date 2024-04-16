
import os
from airflow import DAG
from datetime import timedelta, datetime, timezone
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from psycopg2 import connect
from airflow.providers.postgres.hooks.postgres import PostgresHook



def save_data_to_postgres(**kwargs):
    # CSV 파일 경로를 Airflow Variable로부터 가져옵니다. (예시 경로: '/path/to/your/data_file.csv')
    csv_file_path = "/data/superstore"
    file_name = "Superstore.csv"
    
    # 데이터 디렉토리 경로와 파일명을 결합하여 전체 파일 경로를 생성합니다.
    full_file_path = os.path.join(csv_file_path, file_name)

    # CSV 파일을 DataFrame으로 로드합니다.
    df = pd.read_csv(full_file_path, encoding='iso-8859-1')

    print(df.head())
    print(f"Total rows loaded: {len(df)}")
    
    # 'Order Date' 열을 날짜 타입으로 변환합니다.
    df['Order Date'] = pd.to_datetime(df['Order Date'])

    # 오늘 날짜 데이터만 필터링합니다.
    today = datetime.now().date()
    filtered_df = df[df['Order Date'] == pd.Timestamp(today)]

    print(filtered_df.head())
    print(f"Total rows loaded: {len(filtered_df)}")

    # 필터링된 데이터를 임시 CSV 파일로 저장합니다.
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d")
    temp_csv_path = f"/data/superstore/{dt_string}.csv"
    filtered_df.to_csv(temp_csv_path, index=False, header=True)

    

    # PostgreSQL 연결 정보
    conn_params = {
        'host': '10.0.10.108',
        'port': 5432,
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres'
    }

    # PostgreSQL에 연결
    conn = connect(**conn_params)
    cursor = conn.cursor()

    # COPY 명령어를 구성하여 데이터를 로드
    copy_sql = f"""
        COPY sales_data_eng FROM STDIN WITH CSV HEADER
        DELIMITER ','
    """
    with open(temp_csv_path, 'r', encoding='iso-8859-1') as f:
        cursor.copy_expert(copy_sql, f)

    # 변경 내용 커밋 및 연결 닫기
    conn.commit()
    cursor.close()
    conn.close()

    print("Data loaded into PostgreSQL successfully")

def check_data_in_database(**kwargs):
    # PostgreSQL 연결
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # 쿼리 실행
    query = "SELECT COUNT(*) FROM sales_data_eng;"
    result = hook.get_first(query)
    num_rows = result[0]

    # 결과 출력
    print(f"Number of rows in sales_data_eng table: {num_rows}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['julee@mz.co.kr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('superstore_daily',
        default_args=default_args,
        #schedule_interval='0 11 * * *',
        schedule='@daily',
        catchup=False) as dag:



        upload_data_to_postgres = PythonOperator(
            task_id='save_data_to_postgres',
            python_callable=save_data_to_postgres,
            provide_context=True,
        )

        check_data_task = PythonOperator(
            task_id='check_data_in_database',
            python_callable=check_data_in_database,
        provide_context=True,
        )
        upload_data_to_postgres >> check_data_task
