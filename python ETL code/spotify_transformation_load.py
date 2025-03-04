import json  # Import JSON module to handle JSON data
import boto3  # Import Boto3 to interact with AWS services (S3)
from datetime import datetime  # Import datetime to generate timestamps
from io import StringIO  # Import StringIO to handle CSV in-memory operations
import pandas as pd  # Import Pandas for data manipulation

def album(data):
    album_list = []  # Initialize an empty list to store album details

    for row in data['items']:  # Iterate over all songs in the playlist
        album_id = row['track']['album']['id']  # Extract album ID
        album_name = row['track']['album']['name']  # Extract album name
        album_release_date = row['track']['album']['release_date']  # Extract release date
        album_total_tracks = row['track']['album']['total_tracks']  # Extract total tracks in the album
        album_url = row['track']['album']['external_urls']['spotify']  # Extract album Spotify URL

        album_elements = {  # Store album details in a dictionary
            'album_id': album_id, 
            'name': album_name, 
            'release_date': album_release_date, 
            'total_tracks': album_total_tracks, 
            'url': album_url
        }

        album_list.append(album_elements)  # Append album dictionary to list

    return album_list  # Return list of album details

def artist(data):
    artist_list = []  # Initialize an empty list to store artist details

    for row in data['items']:  # Iterate over all songs in the playlist
        for key, value in row.items():  
            if key == 'track':  
                for artist in value['artists']:  # Loop through all artists in a song
                    artist_dict = {  # Store artist details in a dictionary
                        'artist_id': artist['id'],  
                        'name': artist['name'],  
                        'url': artist['href']
                    }  

                    artist_list.append(artist_dict)  # Append artist dictionary to list

    return artist_list  # Return list of artist details

def songs(data):
    song_list = []  # Initialize an empty list to store song details

    for row in data['items']:  # Iterate over all songs in the playlist
        song_id = row['track']['id']  # Extract song ID
        song_name = row['track']['name']  # Extract song name
        song_duration = row['track']['duration_ms']  # Extract song duration in milliseconds
        song_url = row['track']['external_urls']['spotify']  # Extract song Spotify URL
        song_popularity = row['track']['popularity']  # Extract song popularity score
        song_added = row['added_at']  # Extract timestamp when song was added
        album_id = row['track']['album']['id']  # Extract album ID of the song
        artist_id = row['track']['album']['artists'][0]['id']  # Extract first artist's ID

        song_element = {  # Store song details in a dictionary
            'song_id': song_id,  
            'song_name': song_name,  
            'duration_ms': song_duration,  
            'url': song_url,  
            'popularity': song_popularity,  
            'song_added': song_added,  
            'album_id': album_id,  
            'artist_id': artist_id  
        }  

        song_list.append(song_element)  # Append song dictionary to list

    return song_list  # Return list of song details

def lambda_handler(event, context):
    s3 = boto3.client('s3')  # Initialize an S3 client to interact with S3
    bucket = "spotifyproject-etl"  # Define the bucket name
    key = "raw_data/to_be_processed/"  # Define the folder where JSON files are stored

    spotify_data = []  # Initialize list to store JSON data
    spotify_keys = []  # Initialize list to store processed file keys

    for file in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']:  # List all objects in S3 under the specified prefix
        file_key = file['Key']  # Extract file name (key)
        if file_key.split('.')[-1] == "json":  # Check if the file is a JSON file
            response = s3.get_object(Bucket=bucket, Key=file_key)  # Fetch the file from S3
            content = response['Body']  # Read the content of the file
            jsonObject = json.loads(content.read())  # Convert JSON data into a Python dictionary
            spotify_data.append(jsonObject)  # Store extracted JSON data
            spotify_keys.append(file_key)  # Store processed file key
    
    for data in spotify_data:  # Process each JSON dataset
        album_list = album(data)  # Extract album details
        artist_list = artist(data)  # Extract artist details
        song_list = songs(data)  # Extract song details

        album_df = pd.DataFrame.from_dict(album_list)  # Convert album list to a Pandas DataFrame
        album_df = album_df.drop_duplicates(subset=['album_id'])  # Remove duplicate albums
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])  # Convert release date to datetime

        artist_df = pd.DataFrame.from_dict(artist_list)  # Convert artist list to a Pandas DataFrame
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])  # Remove duplicate artists

        song_df = pd.DataFrame.from_dict(song_list)  # Convert song list to a Pandas DataFrame
        song_df = song_df.drop_duplicates(subset=['song_id'])  # Remove duplicate songs
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])  # Convert song added timestamp to datetime

        song_key = "transformed_data/songs_data/song_transformed_" + str(datetime.now()) + ".csv"  # Define the transformed song file path
        song_buffer = StringIO()  # Create an in-memory buffer for the CSV file
        song_df.to_csv(song_buffer, index=False)  # Convert DataFrame to CSV format, and not have a seprate index column
        song_content = song_buffer.getvalue()  # Retrieve CSV data as a string
        s3.put_object(Bucket=bucket, Key=song_key, Body=song_content)  # Upload the transformed song data to S3

        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"  # Define the transformed artist file path
        artist_buffer = StringIO()  # Create an in-memory buffer for the CSV file
        artist_df.to_csv(artist_buffer, index=False)  # Convert DataFrame to CSV format, and not have a seprate index column
        artist_content = artist_buffer.getvalue()  # Retrieve CSV data as a string
        s3.put_object(Bucket=bucket, Key=artist_key, Body=artist_content)  # Upload the transformed artist data to S3

        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"  # Define the transformed album file path
        album_buffer = StringIO()  # Create an in-memory buffer for the CSV file
        album_df.to_csv(album_buffer, index=False)  # Convert DataFrame to CSV format, and not have a seprate index column
        album_content = album_buffer.getvalue()  # Retrieve CSV data as a string
        s3.put_object(Bucket=bucket, Key=album_key, Body=album_content)  # Upload the transformed album data to S3

    s3_resource = boto3.resource('s3')  # Initialize an S3 resource to manage objects
    for key in spotify_keys:  # Iterate over processed files
        copy_source = {  # Define the source file for copying
            'Bucket': bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, bucket, "raw_data/processed/" + key.split('/')[-1])  # Copy file to processed folder
        s3_resource.Object(bucket, key).delete()  # Delete the original file from "to_be_processed" folder
