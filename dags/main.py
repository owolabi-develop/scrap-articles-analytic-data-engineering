import pendulum
import glob
import csv
import os
import datetime
from get_newsdata import insert_scrap_data,all_scrap_func,folderpath
from airflow.operators.python import PythonOperator
import shutil
from airflow.decorators import dag, task
import  pandas as pd
import boto3
from transform import transform_data
from loaddata import upload_scrap_data_to_space


@dag(
    dag_id="news_articles_process",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def news_articles_process():
    ## extract the new article data
    transformpath ='/opt/airflow/dags/latestArticles/transform'
    extract = PythonOperator(task_id="extract_articles",
                             python_callable=insert_scrap_data,
                             op_kwargs={'all_scrap_func':all_scrap_func}
                             )
    
    transform = PythonOperator(task_id="transform_data",
                               python_callable=transform_data,
                               op_kwargs={'extractpath':folderpath,'transformpath':transformpath}
                               )
    
    load_to_s3 = PythonOperator(task_id="load_to_s3",
                                python_callable=upload_scrap_data_to_space,
                                op_kwargs={'bucket_name':'latestarticles',
                                           'folder_name':"raw-transform-data",
                                           'file_path':'/opt/airflow/dags/latestArticles/transform/tranformdata.json',
                                           'aws_access_key_id':'AKIARDTV5PYPY5DFBB6P',
                                           'aws_secret_access_key':'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'
                                           })
    

    @task
    def remove_dir():
        ## remove the filepath to free resource
        try:
            shutil.rmtree(folderpath)
            #print(f"{folderpath}path remove")
        except:
           pass

    extract >> transform >> load_to_s3 >> remove_dir()
    


dag = news_articles_process()