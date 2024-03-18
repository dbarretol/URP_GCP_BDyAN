# Entry point: cf_IngestCSTable

from google.cloud import storage
from google.cloud import bigquery

def cf_IngestCSTable(event, context):
    """Activado por un cambio en un bucket de almacenamiento en la nube.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    # Define the bucket and blob
    source_bucket = storage_client.bucket("[nombre del bucket]")
    source_blob = source_bucket.blob("[nombre del archivo] por ejemplo orderdetails.csv")

    # Define the BigQuery dataset and table
    dataset_id = 'pf_dtlk_raw'
    table_id = 'orderdetails'
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    # Create a job config
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"

    # Load the data from the blob into BigQuery
    load_job = bigquery_client.load_table_from_uri(
        "gs://...nombre del bucket..../orderdetails.csv",
        table_ref,
        job_config=job_config,
    )

    load_job.result()  # Wait for the job to complete

    print("Loaded {} rows into {}:{}.".format(load_job.output_rows, dataset_id, table_id))
    
# requirements.txt
# # Function dependencies, for example:
# # package>=version
# google-cloud-storage
# google-cloud-bigquery
# google-api-python-client