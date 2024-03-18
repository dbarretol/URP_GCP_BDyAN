#entry point: cf_IngestTable

import mysql.connector as connection
import pandas as pd
from google.cloud import bigquery
import mysql

def cf_IngestTable(request):
  # Request arguments
  request_arg = request.args
  
  prj = request_arg['prj']
  table = request_arg['table']

  # Database connection
  mydb = connection.connect(
    host="xx.xxx.x.xxx", #<-- Aqui va el ip de la base de datos a acceder
    user="abc",#<--Aqui va el usuario para autenficarse en la base de datos
    passwd="pwd",#<--Aqui va el passsword para autenficarse en la base de datos
    database="xyz"#<--Aqui va el nombre de la base de datos donde queremos conectarnos
  )

  # Query execution
  query = f'SELECT * FROM xyz.{table};'
  result_dataFrame = pd.read_sql(query, mydb)
  mydb.close()  # Close the connection 

  # BigQuery client
  client = bigquery.Client()

  # Table configuration
  table_id = f"{prj}.pf_dtlk_raw.{table}"
  table = client.get_table(table_id)

  # Job configuration
  job_config = bigquery.LoadJobConfig(
    schema=table.schema,
    write_disposition="WRITE_TRUNCATE",
  )

  # Load data into BigQuery
  client.load_table_from_dataframe(
    result_dataFrame,
    table_id,
    job_config=job_config)

  return f"La tabla {table} fue ingestada correctamente!"
  

# # Function dependencies, for example:
# # package>=version
# mysql-connector-python
# google-api-python-client
# google-cloud-bigquery
# google-cloud-bigquery-storage
# pandas
# SQLAlchemy
# pyarrow