#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to download completed GEE drought export tasks from Google Drive

Run this after your GEE export tasks are completed to download all files
to your local data/raw directory.
"""
import os
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# Scopes required for accessing Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / 'data' / 'raw' / 'drought'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def authenticate():
    """Authenticate and return Google Drive service"""
    creds = None
    token_file = Path.home() / '.credentials' / 'drive_token.pickle'
    credentials_file = Path(__file__).parent / 'credentials.json'
    
    # Load existing token
    if token_file.exists():
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_file.exists():
                print("ERROR: credentials.json not found!")
                print("\nTo download files from Google Drive, you need to:")
                print("1. Go to: https://console.cloud.google.com/apis/credentials")
                print("2. Create OAuth 2.0 credentials (Desktop app)")
                print("3. Download credentials.json")
                print("4. Save it in this script's directory")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_file), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        token_file.parent.mkdir(parents=True, exist_ok=True)
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def find_earthengine_folder(service):
    """Find the earthengine folder in Google Drive"""
    query = "name='earthengine' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if not items:
        print("ERROR: 'earthengine' folder not found in Google Drive")
        print("Make sure your GEE export tasks have completed.")
        return None
    
    return items[0]['id']

def download_drought_files(service, folder_id, filename_pattern='Iowa_county_drought_DM'):
    """Download all drought files matching the pattern"""
    query = f"'{folder_id}' in parents and name contains '{filename_pattern}' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if not items:
        print(f"No files found matching pattern: {filename_pattern}")
        return
    
    print(f"Found {len(items)} files to download")
    
    for item in items:
        file_id = item['id']
        file_name = item['name']
        output_path = OUTPUT_DIR / file_name
        
        if output_path.exists():
            print(f"  Skipping {file_name} (already exists)")
            continue
        
        print(f"  Downloading {file_name}...")
        request = service.files().get_media(fileId=file_id)
        
        with open(output_path, 'wb') as f:
            from googleapiclient.http import MediaIoBaseDownload
            import io
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                if status:
                    print(f"    Progress: {int(status.progress() * 100)}%")
        
        print(f"    Saved to: {output_path}")

def main():
    print("Authenticating with Google Drive...")
    service = authenticate()
    
    if not service:
        return
    
    print("Finding earthengine folder...")
    folder_id = find_earthengine_folder(service)
    
    if not folder_id:
        return
    
    print(f"Downloading files to: {OUTPUT_DIR}")
    download_drought_files(service, folder_id)
    
    print("\nDownload complete!")

if __name__ == '__main__':
    main()



