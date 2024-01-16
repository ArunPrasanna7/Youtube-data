import pymysql
from pymongo import MongoClient

# Connect to MongoDB
mongo_client = MongoClient("mongodb+srv://arunprasanna7190:7777@cluster0.37z8w81.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_client["Youtube_Demo"]
mongo_collection = mongo_db["Youtube_data"]

# Connect to MySQL
mysql_connection = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="Youtube_project"
)

# Define a function to migrate data
def migrate_data():
    cursor = mysql_connection.cursor()

    # Query MongoDB data
    mongo_data = mongo_collection.find({})

    cursor.execute("DELETE FROM youtube_project.Videos")
    mysql_connection.commit()
   
    for data in mongo_data:
       
        sql_channeldata = '''INSERT INTO Youtube_project.channels (channel_name, subscribers, channel_views, total_videos, channel_desc, playlist_id)
            VALUES (%s, %s, %s, %s, %s, %s)'''
        values_channeldata = (data.get('channel_name'), data.get('subscribers'), data.get('channel_views'), data.get('total_videos'), data.get('channel_desc'), data.get('playlist_id')
                         )
        cursor.execute(sql_channeldata, values_channeldata)
        mysql_connection.commit()
        count=0
        if 'videos' in data and data['videos']:
            for video in data['videos']:
                # Check if the required keys exist in the video document
                count = count +1
                
                if 'video_id' in video and 'title' in video and 'video_desc' in video and 'video_datetime' in video and 'likes' in video and 'comments' in video and 'views' in video and 'favorite' in video and 'duration' in video:
                   sql_videos = '''INSERT INTO Youtube_project.Videos (video_id, channel_name, title,video_desc, video_datetime, likes, comments, views, favorite, duration)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                   values_videos = (video.get('video_id'), data.get('channel_name'), video.get('title'), video.get('video_desc'), video.get('video_datetime'), video.get('likes'), video.get('comments'), video.get('views'), video.get('favorite'), video.get('duration'))   
                   cursor.execute(sql_videos, values_videos)
                   mysql_connection.commit()
    # Commit changes and close connection
    
    cursor.close()

# Call the function to start the migration process
migrate_data()

# Close MongoDB and MySQL connections
mongo_client.close()
mysql_connection.close()
