import sys
import os
sys.path.append(os.environ["DAGS_FOLDER"] + '/lib')
import utils as ut
from airflow.operators.python_operator import PythonOperator


def save_df_fn(ti, **ctx):
    source_task_id, source_key = ctx['source_key'].split('.')

    df = ut.XComHelper.pull_df(ti, task_id=source_task_id, key=source_key)

    path = f'{ctx["target_path"]}.json'
    df.to_json(path)

    return f'DataFrame saved on {path} path.'


class JSONTasks:
    @staticmethod
    def save_df(dag, source_key, target_path, task_id='save-df'):
        return PythonOperator(
            task_id         = task_id,
            python_callable = save_df_fn,
            dag             = dag,
            provide_context = True,
            op_kwargs       = {'source_key': source_key, 'target_path': target_path}
        )