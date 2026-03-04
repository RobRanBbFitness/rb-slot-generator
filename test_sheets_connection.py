import gspread
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE = "service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = "19uB3x5uJgjTtJbVv2qo9ZZicxbjaTuO_qLUFI8-Sn64"
TAB_NAME = "RBSLOT_AUTOMATION"

def main():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    rows = ws.get_all_values()[:8]
    print("Connected ✅")
    print("First rows:")
    for r in rows:
        print(r)

if __name__ == "__main__":
    main()