from dotenv import load_dotenv
import os
from kafka import KafkaProducer
from googleapiclient.discovery import build
import json
import random
from faker import Faker

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

def fetch_comments(video_id, duplication_factor=3):
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
        index = 1
        # Process comments in the current page
        for item in response.get('items', []):
            comment = item['snippet']['topLevelComment']['snippet']
            comment_data = {
                'index': index,
                'video_id': video_id,
                'username': comment['authorDisplayName'],
                'text': comment['textDisplay'],
                'timestamp': comment['publishedAt']
            }
            # Send the comment to the Kafka topic
            producer.send('raw_comments', comment_data)
            print(f"Sent comment from {comment['authorDisplayName']}")
            index += 1

            # Create and stream duplicates
            duplicates, index = duplicate_comments(comment_data, duplication_factor, index)
            for duplicate in duplicates:
                producer.send('raw_comments', duplicate)
                print(f"Streamed duplicate comment from {duplicate['author']} to Kafka.")

        # Check for the next page token
        next_page_token = response.get('nextPageToken')
        if not next_page_token:  # Exit loop if no more pages
            break

    print("All comments fetched and sent to Kafka.")


def duplicate_comments(comments, num_duplicates, index):
    fake = Faker()
    duplicated_comments = []
    for comment in comments:
        for _ in range(num_duplicates):
            new_comment = comment.copy()
            new_comment["index"] = index
            new_comment["username"] = fake.user_name()
            new_comment["timestamp"] = fake.date_time_this_year().isoformat()
            duplicated_comments.append(new_comment)
            index += 1
    return duplicated_comments, index


if __name__ == '__main__':
    video_id = None  # Replace with the YouTube video ID
    if video_id is None:
        generate_fake_comments(1_000_000)
    else:
        fetch_comments(video_id)
