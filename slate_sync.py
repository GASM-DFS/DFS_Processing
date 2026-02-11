import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- SETTINGS ---
FOLDER_PATH = "/home/ava-dfs/Downloads/Ava_Folder"
SHEET_ID = "1JR06-bcrgJmr7QZjoqSaY8jCilGLDH9O5E6oNwohsWk"
PROTECTED_TAB = 'Main Data'
KEY_FILE = os.path.join(FOLDER_PATH, 'service_account.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def run_sync():
    print(f"🚀 [TERMINAL RUN] Scanning: {FOLDER_PATH}")
    
    if not os.path.exists(KEY_FILE):
        print(f"❌ Missing: {KEY_FILE}")
        return

    # Find the CSVs
    files = [f for f in os.listdir(FOLDER_PATH) if "Slate" in f and f.endswith('.csv')]
    
    if not files:
        print("⚠️ No Slate CSVs found to process.")
        return

    # Auth
    creds = Credentials.from_service_account_file(KEY_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    ss = client.open_by_key(SHEET_ID)

    # Clean existing tabs (except Protected)
    for sheet in ss.worksheets():
        if sheet.title != PROTECTED_TAB:
            ss.del_worksheet(sheet)
            print(f"🗑️ Deleted tab: {sheet.title}")

    # Process and Upload
    for file_name in files:
        path = os.path.join(FOLDER_PATH, file_name)
        tab_name = file_name.replace('.csv', '')
        try:
            # Slicing J8:R (Columns 9 to 18, skipping first 7 rows)
            df = pd.read_csv(path, skiprows=7, usecols=range(9, 18), header=None)
            df = df.dropna(subset=[df.columns[0]]) # Drop rows where Col J is empty
            
            new_sheet = ss.add_worksheet(title=tab_name, rows="1000", cols="10")
            new_sheet.update(values=df.values.tolist(), range_name='A1')
            print(f"✅ Uploaded: {tab_name}")
        except Exception as e:
            print(f"⚠️ Failed {file_name}: {e}")

if __name__ == "__main__":
    run_sync()
