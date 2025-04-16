import os
import io
import logging
import argparse
from tqdm import tqdm
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pickle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def authenticate(credentials_file='credentials.json', token_file='token.pkl'):
    """Authenticate with Google Drive API."""
    logger.info("Starting authentication...")
    creds = None
    
    # Load existing token if available
    if os.path.exists(token_file):
        logger.info(f"Loading token from {token_file}")
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)

    # Refresh or create new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing token...")
            creds.refresh(Request())
        else:
            logger.info("Initiating new authentication flow...")
            if not os.path.exists(credentials_file):
                raise FileNotFoundError(f"Credentials file not found: {credentials_file}")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
            logger.info(f"Token saved to {token_file}")

    logger.info("Authentication successful")
    return build('drive', 'v3', credentials=creds)

def download_all_files(service, save_dir='drive_download', page_size=1000):
    """Download all files from Google Drive to specified directory."""
    logger.info(f"Creating download directory: {save_dir}")
    os.makedirs(save_dir, exist_ok=True)
    page_token = None
    file_count = 0

    while True:
        logger.info("Fetching file list...")
        try:
            results = service.files().list(
                pageSize=page_size,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()

            items = results.get('files', [])
            if not items:
                logger.info("No files found")
                break

            logger.info(f"Found {len(items)} files")
            for item in tqdm(items, desc="Downloading files"):
                file_id = item['id']
                file_name = item['name']
                mime_type = item['mimeType']
                path = os.path.join(save_dir, file_name)

                if mime_type.startswith('application/vnd.google-apps'):
                    logger.info(f"Skipping Google Docs file: {file_name}")
                    continue

                try:
                    request = service.files().get_media(fileId=file_id)
                    with open(path, 'wb') as f:
                        downloader = MediaIoBaseDownload(f, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                    logger.info(f"Downloaded: {file_name}")
                    file_count += 1
                except Exception as e:
                    logger.error(f"Failed to download {file_name}: {e}")

            page_token = results.get('nextPageToken')
            if not page_token:
                break

        except Exception as e:
            logger.error(f"Error fetching files: {e}")
            break

    logger.info(f"Total files downloaded: {file_count}")
    return file_count

def main():
    """Main function to parse arguments and run the download process."""
    parser = argparse.ArgumentParser(description='Download files from Google Drive')
    parser.add_argument('--save-dir', default='drive_download', 
                       help='Directory to save downloaded files')
    parser.add_argument('--credentials', default='credentials.json',
                       help='Path to Google API credentials file')
    parser.add_argument('--token', default='token.pkl',
                       help='Path to token file')
    parser.add_argument('--page-size', type=int, default=1000,
                       help='Number of files to fetch per API call')
    
    args = parser.parse_args()

    try:
        service = authenticate(args.credentials, args.token)
        download_all_files(service, args.save_dir, args.page_size)
    except Exception as e:
        logger.error(f"Program failed: {e}")
        exit(1)

if __name__ == '__main__':
    main()