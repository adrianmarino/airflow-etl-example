import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

sys.path.append(os.environ["DAGS_FOLDER"] + '/lib')
import utils as ut
import tasks as ts


# pip dependencies: pandas, pandas-gbq, google-cloud-storage.

def transform_fn(ti):
    df = ut.XComHelper.return_value_df(ti, 'query-iris-table')

    # Make any transformation...

    return df.to_json()

with DAG(
    'big-query-to-bucket',
    default_args       = {
        'owner'           : 'airflow',
        'depends_on_past' : False,
        'email'           : ['adrianmarino@gmail.com'],
        'email_on_failure': False,
        'email_on_retry'  : False,
        'retries'         : 3,
        'retry_delay'     : timedelta(minutes=5)
    },
    description       = """
        Make a query into BigQuery dataset
        and save result both locally and into
        a google cloud bucket.
    """,
    schedule_interval = timedelta(days=1),
    start_date        = datetime(2022, 1, 1),
    catchup           = False,
    tags              = ['big_query_to_bucket']
) as dag:

    t1 = ts.GoogleCloudTasks.query(
        dag,
        task_id = 'query-iris-table',
        query   = """
            SELECT
                *
            FROM
                proven-mind-352922.iris.iris
            LIMIT
                10
        """
    )

    t2 = PythonOperator(
        dag             = dag,
        task_id         = 'transform-table',
        python_callable = transform_fn
    )

    t3 = ts.JSONTasks.save_df(
        dag,
        task_id     = 'save-iris-table-locally',
        source_key  = 'transform-table.return_value',
        target_path = '/var/tmp/iris'
    )

    t4 = ts.GoogleCloudTasks.save_df_to_bucket(
        dag,
        task_id     = 'save-iris-table-into-bucket',
        source_key  = 'transform-table.return_value',
        target_path = 'am-staging-storage/outputs/iris'
    )

    t5 = ts.K8sTasks.create(
        task_id = "run-model-1",
        image   = 'adrianmarino/test.project:lastest',
        cmds    = ["conda", "run", "-n", "project", "python", "/project/test.py"]
    )

    t1 >> t2 >> [t3, t4] >> t5

