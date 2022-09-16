
import os
import typer
from bq_client import BQClient
from postgres_client import PostgresClient

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
PROJECT =  os.environ.get('PROJECT')
DATASET_ID =  os.environ.get('DATASET_ID')
POSTGRESURL =  os.environ.get('POSTGRESURL')

app = typer.Typer()
bqClient = BQClient( project= PROJECT, dataset_id= DATASET_ID)
postgresClient = PostgresClient(postgresURL= POSTGRESURL)

@app.command()
def read_deduplicated_table(table_name : str = typer.Option(  help="name of the table to be synced "),):
    tableDF = bqClient.read_table(table_id= table_name  )
    postgresClient.write_to_postgres(df=tableDF)

if __name__ == "__main__":
    app()
