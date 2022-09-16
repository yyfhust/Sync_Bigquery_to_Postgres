
import os
import typer
from DatabaseConnector.bq_client import BQClient
from DatabaseConnector.postgres_client import PostgresClient

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
PROJECT =  os.environ.get('PROJECT')
DATASET_ID =  os.environ.get('DATASET_ID')
POSTGRESURL =  os.environ.get('POSTGRESURL')

app = typer.Typer()
try :
    bqClient = BQClient( project= PROJECT, dataset_id= DATASET_ID)
    postgresClient = PostgresClient(postgresURL= POSTGRESURL)
except Exception as e:
    print("failed to set up bigquery / postgres connection, please check if you have supplied the service account key and set the env vars. " )
    print(e)

@app.command()
def main(table_name : str = typer.Option(..., prompt="name of the bigquery table and postgres table") ,
         primaryKey: str = typer.Option(..., prompt="primary key of the table")):

    tableDF = bqClient.read_table(table_id= table_name )
    postgresClient.write_to_postgres(df=tableDF, primaryKey= primaryKey, table_name = table_name)


if __name__ == "__main__":
    app()
