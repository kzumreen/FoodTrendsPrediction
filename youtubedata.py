#Step 1: Importing Data and libraries

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone
import isodate 
import os
import pickle
import google.auth.transport.requests
from google_auth_oauthlib.flow import InstalledAppFlow


# --- Your Authentication Function ---
# This function is necessary for the API calls in the next steps.
# Copy and paste the get_authenticated_service() function from your successful setup script here.

SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

def get_authenticated_service():
    """Authenticates using token.pickle or initiates a new OAuth flow."""
    credentials = None
    if os.path.exists('token.pickle'):
        print("Loading credentials from token.pickle...")
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            print("Refreshing expired credentials...")
            credentials.refresh(google.auth.transport.requests.Request())
        else:
            print("Initiating new OAuth 2.0 flow...")
            # Ensure 'client_secret.json' is in your working directory
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES)
            credentials = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            print("Saving credentials to token.pickle...")
            pickle.dump(credentials, token)

    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

#Step 2: Search for Video IDS

def search_videos(youtube, query, max_results=50, published_after=None, video_duration='any'):
    """
    Searches YouTube for video IDs related to a query.

    :param youtube: Authenticated YouTube service object.
    :param query: The search term (e.g., "dalgona coffee").
    :param max_results: Maximum videos to retrieve per page (max 50).
    :param published_after: ISO 8601 timestamp (e.g., '2022-01-01T00:00:00Z').
    :param video_duration: 'short', 'medium', 'long', or 'any'. Use 'short' for Shorts/viral content.
    :return: List of dictionaries containing video IDs and basic info.
    """
    video_data = []
    next_page_token = None

    print(f"Searching for videos with query: '{query}'...")

    # Loop to handle pagination and retrieve multiple results pages
    while True:
        request = youtube.search().list(
            part='snippet',
            q=query,
            maxResults=max_results,
            type='video',
            pageToken=next_page_token,
            publishedAfter=published_after,
            videoDuration=video_duration, 
            order='viewCount' # Sort by view count for relevance/virality
        )

        try:
            response = request.execute()
        except HttpError as e:
            print(f"An HTTP error occurred: {e}")
            break

        for item in response.get('items', []):
            if item['id'].get('videoId'):
                video_data.append({
                    'search_query': query,
                    'video_id': item['id']['videoId'],
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt'],
                    'channel_id': item['snippet']['channelId']
                })

        next_page_token = response.get('nextPageToken')

        # Stop after 500 videos or if no more pages
        if not next_page_token or len(video_data) >= 500: 
            break
        
    print(f"Collected {len(video_data)} video IDs.")
    return video_data

#Step 3: Get Detailed Video Statistics

def get_video_stats(youtube, video_ids):
    """
    Retrieves detailed statistics and metadata for a list of video IDs.
    The API accepts up to 50 IDs per request.

    :param youtube: Authenticated YouTube service object.
    :param video_ids: List of video IDs.
    :return: List of dictionaries with full video data.
    """
    full_stats = []
    
    # Process videos in batches of 50 to maximize efficiency
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        id_string = ','.join(batch)
        
        request = youtube.videos().list(
            # Request the snippet (title, tags), statistics (views, likes), and contentDetails (duration)
            part='snippet,statistics,contentDetails',
            id=id_string
        )
        
        try:
            response = request.execute()
        except HttpError as e:
            print(f"An HTTP error occurred during stats retrieval: {e}")
            continue

        for item in response.get('items', []):
            stats = item.get('statistics', {})
            snippet = item.get('snippet', {})
            content = item.get('contentDetails', {})
            
            full_stats.append({
                'video_id': item.get('id'),
                'title': snippet.get('title'),
                'description': snippet.get('description'),
                'tags': '|'.join(snippet.get('tags', [])), # Join tags into a string
                'published_at': snippet.get('publishedAt'),
                # Convert statistics to integers, defaulting to 0 if missing
                'view_count': int(stats.get('viewCount', 0)),
                'like_count': int(stats.get('likeCount', 0)), 
                'comment_count': int(stats.get('commentCount', 0)),
                'duration_iso': content.get('duration'), # ISO 8601 format (e.g., PT4M20S)
                'scrape_date': datetime.now().isoformat() # Crucial for time-series analysis
            })
            
    print(f"Retrieved detailed statistics for {len(full_stats)} videos.")
    return full_stats

#Step 4: Data Cleaning and Feature Engineering

def clean_and_process_data(df):
    """
    Performs necessary cleaning and feature engineering, fixes the tz-aware error,
    adds the Trend_ID column, and renames 'published_at' to 'Date'.
    """

    # --- Trend ID Mapping Setup ---
    # Create the mapping from query (as found in data) to Trend_ID (as desired for output)
    TREND_MAPPING = {
        query: query.replace(' ', '-') for query in TREND_QUERIES
    }

    # 1. Date/Time Conversion

    # Convert published_at to datetime objects (it will retain its timezone)
    df['published_at'] = pd.to_datetime(df['published_at'])

    # Convert scrape_date to datetime objects
    df['scrape_date'] = pd.to_datetime(df['scrape_date'])

    # --- FIX FOR "Cannot subtract tz-naive and tz-aware" ---
    if df['scrape_date'].dt.tz is None:
        df['scrape_date'] = df['scrape_date'].dt.tz_localize(timezone.utc)

    # Convert both to a common timezone (UTC)
    df['published_at'] = df['published_at'].dt.tz_convert(timezone.utc)
    df['scrape_date'] = df['scrape_date'].dt.tz_convert(timezone.utc)
    # --------------------------------------------------------

    # 2. Duration Conversion (ISO 8601 to Seconds)
    def iso_to_seconds(duration):
        """Converts ISO 8601 duration string to total seconds."""
        try:
            return isodate.parse_duration(duration).total_seconds()
        except:
            return None

    df['duration_seconds'] = df['duration_iso'].apply(iso_to_seconds)

    # 3. Feature Engineering

    # Calculate Engagement Rate: (Likes + Comments) / Views
    df['engagement_rate'] = (df['like_count'] + df['comment_count']) / df['view_count']
    df['engagement_rate'].replace({float('inf'): 0, float('nan'): 0}, inplace=True)

    # Calculate Age at Scrape (This subtraction is now safe)
    df['age_days'] = (df['scrape_date'] - df['published_at']).dt.total_seconds() / (60*60*24)

    # Categorize Duration
    df['video_type'] = pd.cut(df['duration_seconds'],
                             bins=[0, 60, df['duration_seconds'].max() + 1],
                             labels=['Short', 'Longer_Video'],
                             right=False,
                             )

    # Optional: Calculate title length
    df['title_length'] = df['title'].str.len()

    # 4. Add Trend_ID Column and Rename 'published_at'
    
    # 4a. Add Trend_ID
    # This assumes a column like 'search_query' or similar exists and contains the queries.
    # We'll assume the column containing the queries is named 'search_query' for this step.
    # If the column name is different, change 'search_query' below.
    # Default to 'other' if no match is found.
    df['Trend_ID'] = df['search_query'].map(TREND_MAPPING).fillna('other')

    # 4b. Rename 'published_at' to 'Date'
    df.rename(columns={'published_at': 'Date'}, inplace=True)

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    
    print("\nData cleaning and feature engineering complete.")
    return df

#Step 5: Daily Aggregation Function

def aggregate_youtube_data(df):
    """
    Groups the processed YouTube data by Trend_ID and Date to create a daily
    time series summary, as required for the final dataset.
    
    The 'Date' column here represents the date the video was published.
    """
    
    # Define aggregation logic for key numerical columns
    # We sum the total activity (views, likes, comments) and average the feature metrics (engagement, duration)
    agg_funcs = {
        'video_id': 'count',                 # Count of videos published that day
        'view_count': 'sum',                 # Total views from videos published that day
        'like_count': 'sum',                 # Total likes from videos published that day
        'comment_count': 'sum',              # Total comments from videos published that day
        'engagement_rate': 'mean',           # Average engagement rate for that day's videos
        'duration_seconds': 'mean',          # Average duration for that day's videos
        'title_length': 'mean'               # Average title length for that day's videos
    }
    
    # Group by the Trend Identifier and the Date of Publication
    df_agg = df.groupby(['Trend_ID', 'Date']).agg(agg_funcs).reset_index()
    df_agg.rename(columns={'video_id': 'daily_video_count'}, inplace=True)
    
    print("\nDaily aggregation by Trend_ID and Date complete.")
    return df_agg

#Step 6: Main Execution

if __name__ == '__main__':
    
    # 1. Define all trend queries using broad, core terms
    TREND_QUERIES = [
        'feta pasta',  
        'matcha', 
        'dubai chocolate'
    ]
    
    # A list to collect raw data from all trends
    all_raw_data = []

    try:
        # 1. Connect to the API (Assume get_authenticated_service is defined/imported)
        youtube = get_authenticated_service()
        
        print("\n--- STARTING DATA COLLECTION FOR MULTIPLE TRENDS (Broad Search) ---")
        
        # 2. Loop through each trend query
        for trend in TREND_QUERIES:
            print(f"\n---------------------------------------------")
            print(f"COLLECTING DATA FOR TREND: {trend.upper()}")
            print(f"---------------------------------------------")

            # 3. Search for video IDs (Step 1)
            video_snippets = search_videos(
                youtube, 
                query=trend,             # Now using the simple term
                max_results=50, 
                video_duration='short'   # Focused on viral Shorts content
            )
            
            video_ids = [v['video_id'] for v in video_snippets]
            
            if not video_ids:
                print(f"Skipping {trend}: No video IDs found.")
                continue
            
            # 4. Get detailed statistics (Step 2)
            full_video_data = get_video_stats(youtube, video_ids)
            
            # 5. Structure into DataFrame and append
            df_temp = pd.DataFrame(full_video_data)
            
            # CRITICAL: Label the data with the core trend term
            df_temp['search_query'] = trend 
            
            all_raw_data.append(df_temp)


        # --- POST-LOOP PROCESSING ---
        if not all_raw_data:
            print("\nNo data collected from any trend. Exiting.")
            exit()
            
        # 6. Combine all trend data into one raw DataFrame
        df_raw_combined = pd.concat(all_raw_data, ignore_index=True)
        
        # 7. Clean and Engineer Features (Step 3) - Uses the corrected tz-aware function
        df_processed_combined = clean_and_process_data(df_raw_combined.copy())

        # 8. Perform Daily Aggregation (Step 4 of the overall methodology)
        df_final_aggregated = aggregate_youtube_data(df_processed_combined.copy())
        
        # 9. Save the data to CSV files
        PROCESSED_FILE = 'youtube_data_all_trends_aggregated.csv'
        
        # Save the final aggregated DataFrame
        df_processed_combined.to_csv(PROCESSED_FILE, index=False)
        
        print(f"\n\n--- DATA COLLECTION SUCCESSFUL ---")
        print(f"Total rows collected: {len(df_raw_combined)} (raw video records)")
        print(f"Total aggregated rows: {len(df_final_aggregated)} (daily trend records)")
        print(f"Processed data saved to: {PROCESSED_FILE}")
                
    except Exception as e:
        print(f"\nAn unexpected error occurred during the data pipeline: {e}")

