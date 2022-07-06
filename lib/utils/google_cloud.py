from google.cloud import storage
import pandas as pd
import os
from google.oauth2 import service_account


os.environ['GC_CREDENTIALS_PATH'] = f'{os.environ["DAGS_FOLDER"]}/proven-mind-352922-08414cb4ab55.json'
os.environ['GC_PROJECT_ID']       = 'proven-mind-352922'


class GoogleCloud:
    @staticmethod
    def save_df_to_bucket(bucket, path, df):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ['GC_CREDENTIALS_PATH']
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        bucket.blob(f'{path}.json').upload_from_string(df.to_json(), 'application/json')


    @staticmethod
    def query(query):
        print('Query:', query)

        credentials = service_account.Credentials.from_service_account_file(
            os.environ['GC_CREDENTIALS_PATH']
        )

        return pd.read_gbq(
            query,
            project_id  = os.environ['GC_PROJECT_ID'],
            credentials = credentials,
            dialect     = 'standard'
        )
