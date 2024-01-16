# Youtube-data
# Youtube data extract and store in mongodb
!pip install pymongo
!pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib


import googleapiclient.discovery
import pymongo

API_KEY = "AIzaSyBh1KGFhcU_YN9efh_-3NypXJ-Rv7hxt-4"
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = API_KEY
DB_Name = "Youtube_Demo"
Collection_Name = "Youtube_data"

from pymongo.mongo_client import MongoClient
mongo_client = MongoClient("mongodb+srv://arunprasanna7190:7777@cluster0.37z8w81.mongodb.net/?retryWrites=true&w=majority")
database=mongo_client[DB_Name]
collection=database[Collection_Name]

# Build the YouTube Data API service
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=DEVELOPER_KEY)


def store_channel_data_in_mongodb(channel_data):
    try:
        collection.insert_one(channel_data)
        print("Channel data stored in MongoDB successfully!")
    except Exception as e:
        print(f"An error occurred while storing data: {e}")


channel_id = "UCZBr9YvFm3ZSn-PJxHy2I5w"  # Replace with the desired channel ID



def get_channel_data(channel_id, max_results=100):
    try:
        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        )
        response = request.execute()

        if response["items"]:
            channel_info = response["items"][0]
            channel_name = channel_info["snippet"]["title"]
            subscribers = channel_info["statistics"]["subscriberCount"]
            channel_views = channel_info["statistics"]["viewCount"]
            total_videos = channel_info["statistics"]["videoCount"]
            channel_desc = channel_info["snippet"]["description"]
            playlist_id = channel_info["contentDetails"]["relatedPlaylists"]["uploads"]


            videos_request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=max_results
            )
            videos_response = videos_request.execute()

            video_details = []
            for video_item in videos_response["items"]:
                video_id = video_item["snippet"]["resourceId"]["videoId"]
                video_desc = video_item["snippet"]["description"]
                video_datetime = video_item["snippet"]["publishedAt"]
                video_title = video_item["snippet"]["title"]
                video_duration = get_video_duration(video_id)                                               

                video_stats_request = youtube.videos().list(
                    part="statistics",
                    id=video_id
                )
                video_stats_response = video_stats_request.execute()

                video_stats = video_stats_response["items"][0]["statistics"]

                likes = video_stats.get("likeCount", 0)
                comments = video_stats.get("commentCount", 0)
                views = video_stats.get("viewCount", 0)
                favorite = video_stats.get("favoriteCount", 0)

                video_detail = {
                    "video_id": video_id,
                    "title": video_title,
                    "video_desc": video_desc,
                    "video_datetime": video_datetime,
                    "likes": likes,
                    "comments": comments,
                    "views": views,
                    "favorite": favorite,
                    "duration": video_duration
                }
                video_details.append(video_detail)

            channel_data = {
                "channel_name": channel_name,
                "subscribers": subscribers,
                "channel_views": channel_views,
                "total_videos": total_videos,
                "channel_desc": channel_desc,
                "playlist_id": playlist_id,
                "videos": video_details
            }

            return channel_data
        else:
            return None
    except googleapiclient.errors.HttpError as e:
        print("An error occurred:", e)
        return None
def get_video_duration(video_id):
    try:
        video_request = youtube.videos().list(
            part="contentDetails",
            id=video_id
        )
        video_response = video_request.execute()
        duration_str = video_response["items"][0]["contentDetails"]["duration"]
        return convert_duration_to_seconds(duration_str)
    except googleapiclient.errors.HttpError as e:
        print(f"An error occurred while fetching video duration for video {video_id}: {e}")
        return None


def convert_duration_to_seconds(duration_str):
    # Remove the "PT" prefix
    duration_str = duration_str[2:]

    minutes = 0
    seconds = 0

    # Check if 'M' and 'S' are present in the duration string
    if 'M' in duration_str:
        minutes_str, duration_str = duration_str.split('M')
        minutes = int(minutes_str)

    if 'S' in duration_str:
        seconds_str = duration_str.replace('S', '')
        seconds = int(seconds_str)

    total_seconds = minutes * 60 + seconds

    return total_seconds


channel_data = get_channel_data(channel_id, max_results=100)
# Assuming you have the function store_channel_data_in_mongodb defined correctly
store_channel_data_in_mongodb(channel_data)

