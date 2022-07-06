import sys
import os
sys.path.append(os.environ["DAGS_FOLDER"] + '/lib')
import utils as ut
from airflow.operators.python_operator import PythonOperator



def save_df_to_bucket_task_fn(ti, **ctx):
    source_task_id, source_key = ctx['source_key'].split('.')
    target_bucket, target_path = ctx['target_path'].split('/', 1)

    ut.GoogleCloud.save_df_to_bucket(
        bucket           = target_bucket,
        path             = target_path,
        df               = ut.XComHelper.pull_df(ti, task_id=source_task_id, key=source_key)
    )
    return f'DataFrame saved on gs://{ctx["target_path"]}.json.'


class GoogleCloudTasks:
    @staticmethod
    def save_df_to_bucket(dag, source_key, target_path, task_id='save-df-to-bucket'):
        return PythonOperator(
            task_id         = task_id,
            python_callable = save_df_to_bucket_task_fn,
            dag             = dag,
            provide_context = True,
            op_kwargs       = {'source_key' : source_key, 'target_path': target_path}
        )


    @staticmethod
    def query(dag, query, task_id='bq_query'):
        return PythonOperator(
            task_id         = task_id,
            python_callable = lambda ti, **ctx: ut.GoogleCloud.query(ctx["query"]).to_json(),
            dag             = dag,
            provide_context = True,
            do_xcom_push    = True,
            op_kwargs       = {'query': query}
        )