import os
import pandas as pd
import gspread
import warnings
from google.oauth2.service_account import Credentials

# Suppress Deprecation Warnings for a cleaner terminal
warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- Configuration ---
FOLDER_PATH = "/home/ava-dfs/Downloads/Ava_Folder"
SHEET_ID = "1JR06-bcrgJmr7QZjoqSaY8jCilGLDH9O5E6oNwohsWk"
KEY_FILE = 'service_account.json'
PROTECTED_TAB = 'Main Data'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def run_sync():
    print("🚀 Scanning for Slate CSVs...")
    files = [f for f in os.listdir(FOLDER_PATH) if "Slate" in f and f.endswith('.csv')]
    
    if not files:
        print("❌ No 'Slate' files found.")
        return

    try:
        creds = Credentials.from_service_account_file(os.path.join(FOLDER_PATH, KEY_FILE), scopes=SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SHEET_ID)
    except Exception as e:
        print(f"❌ Auth/Connection Error: {e}")
        return

    # Clean up old contest tabs
    for sheet in spreadsheet.worksheets():
        if sheet.title != PROTECTED_TAB:
            spreadsheet.del_worksheet(sheet)
            print(f"🗑️ Removed old tab: {sheet.title}")

    # Process each context
    for file_name in files:
        full_path = os.path.join(FOLDER_PATH, file_name)
        tab_name = file_name.replace('.csv', '')

        try:
            # Slicing J8:R (Columns 9-17, skipping first 7 rows)
            df = pd.read_csv(full_path, skiprows=7, usecols=range(9, 18), header=None)
            
            # Filter: WHERE NOT J IS NULL (J is index 0 in this slice)
            df_filtered = df.dropna(subset=[df.columns[0]])

            # Create tab and upload with modern syntax
            new_sheet = spreadsheet.add_worksheet(title=tab_name, rows="1000", cols="10")
            new_sheet.update(values=df_filtered.values.tolist(), range_name='A1')
            print(f"✅ Success: {tab_name} uploaded.")

        except Exception as e:
            print(f"⚠️ Error on {file_name}: {e}")

if __name__ == "__main__":
    run_sync()
