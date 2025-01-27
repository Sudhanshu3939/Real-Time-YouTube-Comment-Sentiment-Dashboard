from dotenv import load_dotenv
import os
from kafka import KafkaProducer
from googleapiclient.discovery import build
import json


#Access variables from .env file
load_dotenv()
api_key = os.environ.get('YOUTUBE_API_KEY')



# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# YouTube Data API setup
youtube = build('youtube', 'v3', developerKey=api_key)

def fetch_comments(video_id):
    """
    Fetch all comments from a YouTube video and send them to the Kafka topic.
    """
    next_page_token = None  # Initial page token

    while True:
        # Build the API request
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100,  # Fetch 100 comments per request
            pageToken=next_page_token  # Pass the current page token
        )
        response = request.execute()

        # Process comments in the current page
        for item in response.get('items', []):
            comment = item['snippet']['topLevelComment']['snippet']
            comment_data = {
                'video_id': video_id,
                'author': comment['authorDisplayName'],
                'text': comment['textDisplay'],
                'timestamp': comment['publishedAt']
            }
            # Send the comment to the Kafka topic
            producer.send('raw_comments', comment_data)
            print(f"Sent comment from {comment['authorDisplayName']}")

        # Check for the next page token
        next_page_token = response.get('nextPageToken')
        if not next_page_token:  # Exit loop if no more pages
            break

    print("All comments fetched and sent to Kafka.")

if __name__ == '__main__':
    video_id = 'arj7oStGLkU'  # Replace with the YouTube video ID
    fetch_comments(video_id)
