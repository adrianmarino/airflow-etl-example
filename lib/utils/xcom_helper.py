import pandas as pd


class XComHelper:
    @classmethod
    def return_value_df(a_class, ti, task_id):
        return a_class.pull_df(ti, task_id, 'return_value') 


    @staticmethod
    def pull_df(ti, task_id, key):
        json = ti.xcom_pull(key=key, task_ids=task_id)
        return pd.read_json(json)


    @staticmethod
    def push_df(ti, key, df):
        ti.xcom_push(key=key, value=df.to_json())
