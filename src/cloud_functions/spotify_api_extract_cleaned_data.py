# import libs
from datetime import datetime
# import time
import json
from google.cloud import storage
import requests

# transform raw charts
def spotify_api_extract_cleaned_data(request):

  #create today date
  today = datetime.today().strftime('%Y-%m-%d')

  # instance of the storage client
  storage_client = storage.Client()

  # instance of a cleaned bucket in your google cloud storage
  cleaned_bucket = storage_client.get_bucket("spotify_zinovii_project_de_cleaned_bucket")

  # iterate over blobs in raw bucket
  for raw_blob in storage_client.list_blobs('spotify_zinovii_project_de_raw_bucket'):
    if str(raw_blob).split(', ')[1].split('_')[-1][:-5]==today:
      #load json from raw bucket
      raw_json = json.loads(raw_blob.download_as_string(client=None))

      # create list of dicts with track data
      cleaned_data = []
      # create dict with artist data
      artist_data = {}
      chart_position = 1
      
      for track_field in raw_json['tracks']['items']:
        try:
          track_data = {}
          track_data['chart_position'] = chart_position
          track_data['chart_date'] = today
          track_data['chart_country'] = str(raw_blob).split(', ')[1].split('_')[2]
          track_data['track_id'] = track_field['track']['id']
          track_data['track_name'] = track_field['track']['name']
          track_data['track_popularity'] = track_field['track']['popularity']
          cleaned_data.append(track_data)

          for artist in track_field['track']['artists']:
            if track_field['track']['id'] not in artist_data:
              artist_data[track_field['track']['id']] = [artist['name']]
            else:
              artist_data[track_field['track']['id']].append(artist['name'])
        except TypeError:
          continue
        chart_position+=1

      # merge two objects into one
      for track_data in cleaned_data:
          for art_k, art_v in artist_data.items():
              if track_data['track_id']==art_k:
                  track_data['artist_name'] = art_v

      # convert to newline delimited JSON
      cleaned_data = [json.dumps(record) for record in cleaned_data]
      cleaned_data = "\n".join(cleaned_data)

      #create a new file in cleaned bucket
      blob = cleaned_bucket.blob(str(raw_blob).split(', ')[1])

      # uploading data using upload_from_string method
      blob.upload_from_string(cleaned_data)

  requests.post(CLOUD_FUNCTION_SPOTIFY_API_LOAD_DATA_TO_BIGQUERY_URL)
  return "charts data were succefully moved to cleaned bucket"
