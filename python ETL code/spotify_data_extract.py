import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Retrieve Spotify API credentials from AWS Lambda environment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    # Authenticate with Spotify using Client Credentials Flow
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    playlists = sp.user_playlists('313odiyd72vqxinsh4grr4wqqfpa')  # The user_playlists() function in the Spotipy library is used to retrieve a list of public playlists created by a specific Spotify user.

    # Define the Spotify playlist link and extract the playlist ID
    playlist_link = "https://open.spotify.com/playlist/34NbomaTu7YuOYnky8nLXL"
    playlist_id = playlist_link.split("/")[-1]

    data = sp.playlist_items(playlist_id) # Fetch all items (tracks and details) from the specified playlist

    client = boto3.client('s3')  # The boto3.client() function is used to create a low-level service client for interacting with AWS services.

    filename = "spotify_raw_" + str(datetime.now()) + ".json"  # Generate a filename with a timestamp to ensure uniqueness

    client.put_object(   # The put_object() method is used to upload an object (file) to an Amazon S3 bucket.
        Bucket='spotifyproject-etl',    # The name of the S3 bucket where the file will be uploaded.
        Key='raw_data/to_be_processed/' + filename,    # The name of the file in S3 (including the path, if needed).
        Body=json.dumps(data)    # The actual data to be uploaded, converted to JSON format using json.dumps().
    )
