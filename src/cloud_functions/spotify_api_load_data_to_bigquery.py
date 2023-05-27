# import lib
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import time

def spotify_api_load_data_to_bigquery(request):
  '''Load data to Bigquery
  '''
  # create today date
  today = datetime.today().strftime('%Y-%m-%d')

  # instance of the storage client
  storage_client = storage.Client()

  # Construct a BigQuery bigquery_client object.
  bigquery_client = bigquery.Client()

  for clean_blob in storage_client.list_blobs('spotify_zinovii_project_de_cleaned_bucket'):

    # check for today files
    if str(clean_blob).split(', ')[1].split('_')[-1][:-5]==today:

      # definy path to table
      table_id = f"{BIGQUERY_DATASET}.{str(clean_blob).split(', ')[1][:-5]}"

      # setup schema for new table
      schema=[
              bigquery.SchemaField("chart_position", "INT64", mode="REQUIRED"),
              bigquery.SchemaField("chart_date", "DATE", mode="REQUIRED"),
              bigquery.SchemaField("chart_country", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("track_id", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("track_name", "STRING"),
              bigquery.SchemaField("track_popularity", "INT64"),
              bigquery.SchemaField("artist_name", "STRING", mode="REPEATED")
          ]
      # definy source table
      uri = f"gs://{CLEANED_BUCKET}/{str(clean_blob).split(', ')[1]}"

      try:
        if bigquery_client.get_table(table_id):
          # bigquery_client.delete_table(table_id, not_found_ok=True)

          # setup upload json
          job_config = bigquery.LoadJobConfig(
          source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
          schema=schema,
          write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
          )
          
          # Make an API request.
          load_job = bigquery_client.load_table_from_uri(
            uri,
            table_id,
            location=LOCATION,  # Must match the destination dataset location.
            job_config=job_config,
          )

          # Waits for the job to complete.
          load_job.result()
          # time.sleep(30)
      except:

        # assign schema to table
        table = bigquery.Table(table_id, schema=schema)

        # Clustering 
        table.clustering_fields = ["track_name"]

        # Partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            field="chart_date"  # name of column to use for partitioning
        )
      
        # create new table
        table = bigquery_client.create_table(table)

        # setup upload json
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )

        # Make an API request.
        load_job = bigquery_client.load_table_from_uri(
            uri,
            table_id,
            location=LOCATION,  # Must match the destination dataset location.
            job_config=job_config,
        )

        # Waits for the job to complete.
        load_job.result()
        # time.sleep(30)
          
  return f"New tables data were succefully upload to bigquery"
