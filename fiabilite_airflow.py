from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from factores.scripts import remove_local_file, local_to_s3
import pandas as pd
import json

path = Variable.get("path_posts")
compression = Variable.get("compression_posts")
filename = Variable.get("filename_posts")
bucket_name = Variable.get("bucket_name_posts")

default_args = {
    'owner': 'YuriFardel',
    'depends_on_past': False,
    'retries': 1
}

def processing_posts(ti, path, ds, filename, compression):
    posts = ti.xcom_pull(task_ids=['get_posts'])
    pd.set_option('display.max_columns', None)
    if not len(posts) :
        raise ValueError('post is empty')
    df = pd.json_normalize(posts)
    df.info()
    df2 = pd.DataFrame(df, dtype=str)
    df2.info()
    df2.columns = df2.columns.astype(str)
    print('df2.head: ', df2.head())
    df2.to_parquet(path=path + filename, compression=compression, index=False)

with DAG(
        'fiabilite_airflow',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False,
        start_date=datetime.now()
    ) as dag:
    
    api_available = HttpSensor(
        task_id = 'api_available',
        http_conn_id = 'posts_api',
        endpoint = '/posts'
    )
    get_posts = SimpleHttpOperator(
        task_id = 'get_posts',
        http_conn_id = 'posts_api',
        endpoint = '/posts',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )
    processing_posts = PythonOperator(
        task_id = 'processing_posts',
        python_callable = processing_posts,
        op_kwargs = {
            'path': path,
            'filename': filename,
            'compression': compression
        }
    )
    upload_to_s3 = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable = local_to_s3,
        op_kwargs = {
            'bucket_name': bucket_name,
            'dir_target': './data',
            'filepath': './data/*.parquet'
        }
    )

    remove_df_local = PythonOperator(
        task_id = 'remove_df_local',
        python_callable = remove_local_file,
        op_kwargs = {
            'filepath': './data/*.parquet'
        }
    )



api_available >> get_posts >> processing_posts >> upload_to_s3 >> remove_df_local