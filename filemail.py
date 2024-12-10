from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
import datetime
from email.mime.text import MIMEText
import base64
from googleapiclient.errors import HttpError
import concurrent.futures
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/gmail.send']

def authenticate():
    """Handles authentication and returns credentials."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def get_drive_service(creds):
    """Builds the Google Drive API service."""
    return build('drive', 'v3', credentials=creds)

def get_gmail_service(creds):
    """Builds the Gmail API service."""
    return build('gmail', 'v1', credentials=creds)

def fetch_files(service, query, size_filter=None, limit=1000):
    """Fetches files from Google Drive based on a query with a limit on the number of files."""
    files = []
    page_token = None
    count = 0

    def fetch_page(page_token):
        try:
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, size, modifiedTime, parents)',
                pageToken=page_token,
                pageSize=1000  # Increase the page size to reduce the number of API calls
            ).execute()
            return response
        except HttpError as error:
            logging.error(f"An error occurred while fetching files: {error}")
            return None

    while True:
        response = fetch_page(page_token)
        if response is None:
            break
        for file in response.get('files', []):
            if size_filter and int(file.get('size', 0)) <= size_filter:
                continue
            files.append(file)
            count += 1
            if count % 100 == 0:  # Log progress every 100 files
                logging.info(f"Fetched {count} files so far...")
            if count >= limit:
                return files
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    logging.info(f"Total files fetched: {count}")
    return files

def send_email(gmail_service, message_text, recipient):
    """Sends an email using Gmail API."""
    message = MIMEText(message_text, 'html')
    message['to'] = recipient
    message['subject'] = 'Google Drive Report'
    encoded_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
    try:
        send_message = gmail_service.users().messages().send(userId="me", body=encoded_message).execute()
        logging.info(f"Message Id: {send_message['id']}")
    except HttpError as error:
        logging.error(f"An error occurred while sending email: {error}")

def process_duplicate_files(files):
    """Processes and groups duplicate files."""
    file_groups = {}
    for file in files:
        key = (file['name'], file['size'])
        file_groups.setdefault(key, []).append(file)
    return file_groups

def main():
    creds = authenticate()
    service = get_drive_service(creds)
    gmail_service = get_gmail_service(creds)

    now = datetime.datetime.now(datetime.timezone.utc)
    twelve_months_ago = now - datetime.timedelta(days=365)
    twenty_four_months_ago = now - datetime.timedelta(days=730)
    twelve_months_ago_str = twelve_months_ago.isoformat()
    twenty_four_months_ago_str = twenty_four_months_ago.isoformat()

    # 1. Large files not modified for 12 months
    large_files_query = f"(modifiedTime < '{twelve_months_ago_str}')"
    logging.info("Fetching large files...")
    large_files = fetch_files(service, large_files_query, size_filter=10485760)
    logging.info(f"Fetched {len(large_files)} large files.")

    # 2. Google Docs not modified for 24 months
    old_docs_query = f"(mimeType='application/vnd.google-apps.document') and (modifiedTime < '{twenty_four_months_ago_str}')"
    logging.info("Fetching old Google Docs...")
    old_docs = fetch_files(service, old_docs_query)
    logging.info(f"Fetched {len(old_docs)} old Google Docs.")

    # 3. Duplicate files over 10MB
    duplicate_files_query = f"(modifiedTime < '{twelve_months_ago_str}')"
    logging.info("Fetching potential duplicate files...")
    duplicate_files_raw = fetch_files(service, duplicate_files_query, size_filter=10485760)
    file_groups = process_duplicate_files(duplicate_files_raw)
    duplicate_files = [files for files in file_groups.values() if len(files) > 1]
    logging.info(f"Found {len(duplicate_files)} groups of duplicate files.")

    # Construct the email report
    report = f"""
    <html>
      <body>
        <h1>Google Drive Report</h1>
        <h2>Large Files (over 10MB) not modified for 12 months:</h2>
        <ul>
          {''.join(f"<li>{file['name']} ({file['size']} bytes, last modified {file['modifiedTime']})</li>" for file in large_files)}
        </ul>
        <h2>Google Docs not modified for 24 months:</h2>
        <ul>
          {''.join(f"<li>{file['name']} (last modified {file['modifiedTime']})</li>" for file in old_docs)}
        </ul>
        <h2>Duplicate Files (over 10MB):</h2>
        <ul>
          {''.join(f"<li>{key[0]} ({key[1]} bytes):<ul>{''.join(f'<li>ID: {f['id']} (parent folder: {f.get('parents', ['unknown'])[0]})</li>' for f in files)}</ul></li>" for key, files in file_groups.items() if len(files) > 1)}
        </ul>
      </body>
    </html>
    """

    recipient_email = 'me@gmail.com'  # Update this with the recipient's email
    logging.info("Sending email report...")
    send_email(gmail_service, report, recipient_email)
    logging.info("Email report sent successfully.")

if __name__ == '__main__':
    main()
