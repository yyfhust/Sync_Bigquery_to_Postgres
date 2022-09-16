
from google.cloud import bigquery
import pandas as pd

class BQClient:
    def __init__(self, project, dataset_id ) -> None:
        self.project = project
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project = project )

    def read_table (self, table_id ) -> None:

        QUERY = """
        SELECT *
        FROM `{project}.{dataset}.{table_id}`
        """.format(project = self.project, dataset = self.dataset_id, table_id = table_id  )
        query_job = self.client.query(QUERY)
        df = query_job.to_dataframe()
        df = df.where(pd.notnull(df), None)
        print(str(len(df)) + " rows in totall from bigquery table")
        return df
