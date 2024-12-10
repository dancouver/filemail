from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
import datetime
from email.mime.text import MIMEText
import base64
from googleapiclient.errors import HttpError
import schedule
import time

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

def fetch_files(service, query, size_filter=None, limit=100):
    """Fetches files from Google Drive based on a query with a limit on the number of files."""
    files = []
    page_token = None
    count = 0

    def fetch_page(page_token):
        try:
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, size, modifiedTime, parents, webViewLink)',
                pageToken=page_token,
                pageSize=1000  # Increase the page size to reduce the number of API calls
            ).execute()
            return response
        except HttpError as error:
            print(f"An error occurred while fetching files: {error}")
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
            if count >= limit:
                return files
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    return files

def send_email(gmail_service, message_text, recipient):
    """Sends an email using Gmail API."""
    message = MIMEText(message_text, 'html')
    message['to'] = recipient
    message['subject'] = 'Google Drive Report'
    encoded_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
    try:
        send_message = gmail_service.users().messages().send(userId="me", body=encoded_message).execute()
        print(f"Message Id: {send_message['id']}")
    except HttpError as error:
        print(f"An error occurred while sending email: {error}")

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
    large_files_query = f"modifiedTime < '{twelve_months_ago_str}'"
    large_files = fetch_files(service, large_files_query, size_filter=20971520)  # 20MB = 20971520 bytes

    # 2. Google Docs not modified for 24 months
    old_docs_query = f"mimeType='application/vnd.google-apps.document' and modifiedTime < '{twenty_four_months_ago_str}'"
    old_docs = fetch_files(service, old_docs_query)

    # Construct the email report
    report = f"""
    <html>
      <body>
        <h1>Google Drive Report</h1>
        <h2>Large Files (over 20MB) not modified for 12 months:</h2>
        <h3>Limited to a fetch of 100 files </h3>                                                        
        <ul>
          {''.join(f"<li>{file['name']} ({file['size']} bytes, last modified {file['modifiedTime']})</li>" for file in large_files)}
        </ul>
        <h2>Google Docs not modified for 24 months:</h2>
        <ul>
          {''.join(f"<li><a href='{file['webViewLink']}'>{file['name']}</a> (last modified {file['modifiedTime']})</li>" for file in old_docs)}
        </ul>
      </body>
    </html>
    """

    recipient_email = 'danhathway@gmail.com'  # Update this with the recipient's email
    send_email(gmail_service, report, recipient_email)
    print("Email report sent successfully.")

# Run the program immediately the first time
main()

# Run only on the first day of the month
def run_monthly():
    if datetime.datetime.now().day == 1:  # Check if it's the 1st of the month
        main()

schedule.every().day.at("10:00").do(run_monthly)

while True:
    schedule.run_pending()
    time.sleep(1)