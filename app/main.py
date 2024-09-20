import os.path
from datetime import datetime
import json
from email.utils import parsedate_to_datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from textblob import TextBlob

from sqlalchemy import Column, String, Text, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def connect_to_postgres(db_config):
    engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config.get('port', 5432)}/{db_config['database']}"
    )
    Session = sessionmaker(bind=engine)
    return engine, Session()

Base = declarative_base()

class Email(Base):
    __tablename__ = 'emails'

    id = Column(String(255), primary_key=True)
    subject = Column(Text)
    body = Column(Text)
    date = Column(DateTime, nullable=True)
    sender = Column(Text)
    category = Column(String(50))
    
def create_tables(engine):
    Base.metadata.create_all(engine)

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def authenticate_gmail():
    creds = None
    if os.path.exists('secrets/token.json'):
        creds = Credentials.from_authorized_user_file('secrets/token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'secrets/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('secrets/token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def load_config(config_file='config.json'):
    with open(config_file, 'r') as json_file:
        return json.load(json_file)

def is_banned(subject, body, banned_keywords, sender, banned_senders):
    if any(keyword.lower() in subject.lower() or keyword.lower() in body.lower() for keyword in banned_keywords):
        return True
    if any(banned_sender.lower() in sender.lower() for banned_sender in banned_senders):
        return True
    return False

def fetch_and_classify_emails(service, banned_keywords, banned_senders, last_pulled_date, confirmation_keywords, rejection_keywords, session):
    query = f'after:{last_pulled_date} category:primary'

    emails_processed = 0
    next_page_token = None
    while True:
        if next_page_token:
            results = service.users().messages().list(userId='me', q=query, maxResults=500, pageToken=next_page_token).execute()
        else:
            results = service.users().messages().list(userId='me', q=query, maxResults=500).execute()

        messages = results.get('messages', [])
        next_page_token = results.get('nextPageToken')

        for message in messages:
            emails_processed += 1
            if emails_processed % 100 == 0:
                print(f'Processed {emails_processed} emails.')

            msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            headers = msg['payload']['headers']
            subject = next((item['value'] for item in headers if item['name'] == 'Subject'), 'No Subject')
            date = next((item['value'] for item in headers if item['name'] == 'Date'), None)
            sender = next((item['value'] for item in headers if item['name'] == 'From'), 'Unknown Sender')
            body = msg['snippet']


            if is_banned(subject, body, banned_keywords, sender, banned_senders):
                continue

            # Check if email already exists
            if email_exists(session, message['id']):
                continue

            classification = classify_email(subject, body, confirmation_keywords, rejection_keywords)

            email_record = {
                'id': message['id'],
                'subject': subject,
                'body': body,
                'date': date,
                'sender': sender
            }

            insert_email(session, email_record, classification)

        if not next_page_token:
            break


    session.commit()


def classify_email(subject, body, confirmation_keywords, rejection_keywords):
    text = subject + " " + body
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity

    if any(keyword.lower() in text.lower() for keyword in confirmation_keywords):
        return "Application Confirmation"
    elif sentiment < 0.1 and any(keyword.lower() in text.lower() for keyword in rejection_keywords):
        return "Rejection"
    elif sentiment > 0.1 and "interview" in text.lower():
        return "Interview Request"
    else:
        return "Other"

def load_existing_data(filename='classified_emails.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as json_file:
            data = json.load(json_file)
            return data
    else:
        return {
            "Application Confirmation": [],
            "Rejection": [],
            "Interview Request": [],
            "counts": {
                "Application Confirmation": 0,
                "Rejection": 0,
                "Interview Request": 0
            },
            "last_pulled_date": '2024/09/01'
        }
        
def insert_email(session, email_data, category):
    try:
        email_date = parsedate_to_datetime(email_data['date'])
    except Exception as e:
        print(f"Error parsing date: {email_data['date']} - {e}")
        email_date = None  
    email = Email(
        id=email_data['id'],
        subject=email_data['subject'],
        body=email_data['body'],
        date=email_date,
        sender=email_data['sender'],
        category=category
    )
    session.add(email)



def email_exists(session, email_id):
    return session.query(Email).filter(Email.id == email_id).first() is not None

def main():
    creds = authenticate_gmail()
    service = build('gmail', 'v1', credentials=creds)

    config = load_config()
    banned_keywords = config['banned_keywords']
    banned_senders = config['banned_senders']
    confirmation_keywords = config['confirmation_keywords']
    rejection_keywords = config['rejection_keywords']
    db_config = config['db_config']

    # Connect to PostgreSQL 
    engine, session = connect_to_postgres(db_config)

    # Create tables if they don't exist
    create_tables(engine)

    # Set last pulled date (you may want to store this in the database)
    last_pulled_date = '2024/09/01'  # Update as needed

    # Fetch and classify emails
    fetch_and_classify_emails(
        service,
        banned_keywords,
        banned_senders,
        last_pulled_date,
        confirmation_keywords,
        rejection_keywords,
        session
    )
    
    session.close()


if __name__ == '__main__':
    main()
