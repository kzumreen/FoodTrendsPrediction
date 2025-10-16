import os
import google_auth_oauthlib.flow
import google.auth.transport.requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import pickle # To store and load credentials

# --- 1. Define OAuth Scopes ---
# The scope defines what kind of access your application needs.
# 'https://www.googleapis.com/auth/youtube.readonly' allows read-only access to YouTube data.
# For more scopes, refer to: https://developers.google.com/youtube/v3/guides/auth/scopes
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

# --- 2. Load Client Secrets ---
# These are obtained from your Google Cloud Project.
# They should be stored in a file named 'client_secrets.json' or loaded from environment variables.
# For simplicity, we'll assume a 'client_secrets.json' file for desktop applications.
# If you want to use environment variables, you'd load them differently here.

# --- 3. Authentication Function ---
def get_authenticated_service():
    credentials = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        print("Loading credentials from token.pickle...")
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            print("Refreshing expired credentials...")
            credentials.refresh(google.auth.transport.requests.Request())
        else:
            print("Initiating new OAuth 2.0 flow...")
            # Create an OAuth 2.0 flow instance for a desktop application.
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES)
            credentials = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            print("Saving credentials to token.pickle...")
            pickle.dump(credentials, token)

    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

# --- Main execution ---
if __name__ == '__main__':
    try:
        print("Attempting to authenticate with OAuth 2.0...")
        youtube = get_authenticated_service()

        # Make a simple, harmless request to search for one video about "data science"
        # This now uses the authenticated 'youtube' service.
        request = youtube.search().list(
            part='snippet',
            q='data science',
            maxResults=1,
            type='video'
        )
        response = request.execute()

        # --- Report Success ---
        video_title = response['items'][0]['snippet']['title']
        print("\n--- SUCCESS! ---")
        print("Connection to YouTube API with OAuth 2.0 is working correctly.")
        print(f"Found a video titled: '{video_title}'")

    except HttpError as e:
        print("\n--- API CONNECTION FAILED ---")
        print("The connection to YouTube failed with OAuth 2.0.")
        print("Error details:", e.content.decode('utf-8'))
        print("\nCommon reasons for this error:")
        print("  1. The 'YouTube Data API v3' is not enabled in your Google Cloud project.")
        print("  2. Incorrect client_secrets.json file or misconfigured OAuth consent screen.")
        print("  3. Issues with the granted permissions or network connectivity.")
    except FileNotFoundError:
        print("\n--- ERROR ---")
        print("client_secrets.json not found.")
        print("Please ensure your OAuth 2.0 client configuration file is present and named 'client_secrets.json' in the same directory as this script.")
        print("Instructions on how to get it are provided below.")
    except Exception as e:
        print("\n--- An unexpected error occurred ---")
        print(f"Error: {e}")