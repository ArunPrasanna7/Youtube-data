import streamlit as st
import pandas as pd
import pymysql

# Connect to MySQL
mysql_connection = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="Youtube_project"
)


# Define a function to execute queries and return results as a DataFrame
def execute_query(query):
    return pd.read_sql(query, mysql_connection)


st.title("Youtube data Analysis")


selection_query = st.selectbox( 'Select question:', ['1.What are the names of all the videos and their corresponding channels?',
'2.Which channels have the most number of videos, and how many videos do they have?',
'3.What are the top 10 most viewed videos and their respective channels?',
'4.How many comments were made on each video, and what are their corresponding video names?',
'5.Which videos have the highest number of likes, and what are their corresponding channel names?',
'6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7.What is the total number of views for each channel, and what are their corresponding channel names?',
'8.What are the names of all the channels that have published videos in the year 2022?',
'9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10.Which videos have the highest number of comments, and what are their corresponding channel names?'
])

if selection_query == '1.What are the names of all the videos and their corresponding channels?':
   query_result = execute_query("SELECT channel_name, title FROM Videos")


elif selection_query == '2.Which channels have the most number of videos, and how many videos do they have?':
   query_result = execute_query("SELECT channel_name, total_videos FROM channels")

elif selection_query == '3.What are the top 10 most viewed videos and their respective channels?':
   query_result = execute_query("SELECT channel_name, title, views FROM Videos ORDER BY views DESC LIMIT 10")


elif selection_query == '4.How many comments were made on each video, and what are their corresponding video names?':
   query_result = execute_query("SELECT title, comments FROM Videos")

elif selection_query == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
   query_result = execute_query("SELECT channel_name, title, likes FROM Videos ORDER BY likes DESC LIMIT 10")

elif selection_query == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
   query_result = execute_query("SELECT channel_name, title, likes FROM Videos")

elif selection_query == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
   query_result = execute_query("SELECT channel_name, channel_views FROM Channels")

elif selection_query == '8.What are the names of all the channels that have published videos in the year 2022?':
   query_result = execute_query("SELECT DISTINCT channel_name FROM videos WHERE YEAR(video_datetime) = 2023")

elif selection_query == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
   query_result = execute_query("SELECT channel_name, AVG(duration) AS average_duration FROM videos GROUP BY channel_name")

elif selection_query == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
   query_result = execute_query("SELECT channel_name, title, comments FROM videos ORDER BY comments DESC LIMIT 10")


st.header(selection_query)
st.table(query_result)

# Close MySQL connection
mysql_connection.close()
