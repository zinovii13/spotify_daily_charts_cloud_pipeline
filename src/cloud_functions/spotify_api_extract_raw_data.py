# import libs
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
# import time
import json
from google.cloud import storage
import requests

def spotify_api_extract_raw_data(request):
  ''' Pull charts data (.json) per Spotify API (with Spotipy) to Cloud Storage
  '''
  # get api authorization
  spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

  # instance of the storage client
  storage_client = storage.Client()

  # instance of a bucket in your google cloud storage
  bucket = storage_client.get_bucket("spotify_zinovii_project_de_raw_bucket")

  # create today timestamp
  today = datetime.today().strftime('%Y-%m-%d')

  # dict charts
  playlist_by_country = {'ua':'37i9dQZEVXbKkidEfWYRuD', 'global': '37i9dQZEVXbMDoHDwVN2tF', 'pl' : '37i9dQZEVXbN6itCcaL3Tt', 'sk': '6WrpeNyqtoq6PEDXyuamGt', 'hu': '37i9dQZEVXbNHwMxAkvmF8', 'ro': '37i9dQZEVXbNZbJ6TZelCq'}

  for country, playlist_id in playlist_by_country.items():

    # create a new file 
    blob = bucket.blob(f'top_50_{country}_{today}.json')

    # uploading data using upload_from_string method
    # json.dumps() serializes a dictionary object as string
    blob.upload_from_string(json.dumps(spotify.playlist(playlist_id=playlist_id)))

  requests.post(CLOUD_FUNCTION_SPOTIFY_API_EXTRACT_CLEANED_DATA_URL)
  return {'response' : 'charts were succefully uploaded to bucket'}
