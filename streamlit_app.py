import json
import re
from datetime import timezone, timedelta
from typing import List

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

try:
    from googleapiclient.discovery import build
except ImportError:
    build = None


# ---------------- HELPER FUNCTIONS ---------------- #

def get_gc_and_creds(sa_json_text: str):
    """Authenticate and return (gspread_client, google_credentials)."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = json.loads(sa_json_text)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc, creds


def open_ws_by_url(gc, url: str, tab: str):
    sh = gc.open_by_url(url)
    ws = sh.worksheet(tab)
    return sh, ws


def read_df(ws) -> pd.DataFrame:
    """Read dataframe starting from row 5 as headers."""
    all_values = ws.get_all_values()
    if len(all_values) < 5:
        raise ValueError("Not enough rows to read headers from row 5.")
    headers = all_values[4]  # row 5 in 0-based index
    data = all_values[5:]    # from row 6 onwards
    df = pd.DataFrame(data, columns=headers)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def normalize_folder_id(s: str) -> str:
    if not s:
        return ""
    if "/folders/" in s:
        return s.split("/folders/")[1].split("/")[0].split("?")[0]
    return s


def normalize_sheet_url(u: str) -> str:
    m = re.search(r"https?://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9\-_]+)", u)
    if not m:
        raise ValueError(f"Not a Google Sheets URL: {u}")
    return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/edit"


def ensure_destination_sheet(gc, creds, dest_sheet_url: str, fallback_name: str = "Accumulated Report") -> str:
    if dest_sheet_url:
        return dest_sheet_url
    if build is None:
        raise RuntimeError("google-api-python-client not installed.")
    drive = build("drive", "v3", credentials=creds)
    file = drive.files().create(
        body={"name": fallback_name, "mimeType": "application/vnd.google-apps.spreadsheet"},
        fields="id"
    ).execute()
    return f"https://docs.google.com/spreadsheets/d/{file['id']}/edit"


def summarize_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()

    base_cols = list(frames[0].columns)
    for i, df in enumerate(frames[1:], start=2):
        if list(df.columns) != base_cols:
            raise ValueError(
                f"Sheet #{i} columns differ from template.\nExpected {base_cols}\nGot {list(df.columns)}"
            )

    all_df = pd.concat(frames, ignore_index=True)

    # Keep only A, B, C, D, J if present
    columns_to_keep = ["A", "B", "C", "D", "J"]
    filtered_cols = [c for c in columns_to_keep if c in all_df.columns]
    return all_df[filtered_cols]


def write_to_dest(gc, dest_sheet_url: str, dest_tab: str, df: pd.DataFrame):
    sh = gc.open_by_url(dest_sheet_url)
    try:
        ws = sh.worksheet(dest_tab)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        rows = max(len(df) + 5, 100)
        cols = max(len(df.columns) + 2, 26)
        ws = sh.add_worksheet(title=dest_tab, rows=rows, cols=cols)
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)


# ---------------- STREAMLIT UI ---------------- #

st.set_page_config(page_title="Sheets Accumulator", page_icon="📊", layout="centered")
st.title("📊 Google Sheets Accumulator (Simple Version)")

with st.expander("1) Authentication", expanded=True):
    default_secret = st.secrets.get("SA_JSON", None)
    sa_source = st.radio("Service Account JSON:", ["Use st.secrets", "Paste JSON"], index=0 if default_secret else 1)
    if sa_source == "Paste JSON":
        sa_json_text = st.text_area("Paste Service Account JSON here", height=160)
    else:
        sa_json_text = default_secret or ""

with st.expander("2) Sources", expanded=True):
    src_mode = st.radio("Source Mode", ["Folder ID", "List of Sheet URLs"], index=0)
    if src_mode == "Folder ID":
        folder_id = normalize_folder_id(st.text_input("Google Drive Folder ID or URL"))
        sheet_urls = []
    else:
        urls_raw = st.text_area("One Google Sheet URL per line", height=120)
        sheet_urls, bad = [], []
        for u in urls_raw.splitlines():
            if u.strip():
                try:
                    sheet_urls.append(normalize_sheet_url(u.strip()))
                except ValueError as e:
                    bad.append(str(e))
        if bad:
            st.error("Invalid URLs:\n" + "\n".join(bad))
        folder_id = ""
    source_tab = st.text_input("Source Tab Name", value="RawData")

with st.expander("3) Destination", expanded=True):
    dest_sheet_url = st.text_input("Destination Sheet URL (leave blank to create new)")
    dest_tab = st.text_input("Destination Tab Name", value="Report_All")

if st.button("🚀 Run Accumulation"):
    try:
        if not sa_json_text:
            st.error("Missing Service Account JSON.")
            st.stop()

        gc, creds = get_gc_and_creds(sa_json_text)

        frames = []
        if src_mode == "List of Sheet URLs":
            for u in sheet_urls:
                _, ws = open_ws_by_url(gc, u, source_tab)
                frames.append(read_df(ws))
        else:
            if build is None:
                st.error("google-api-python-client not installed.")
                st.stop()
            drive = build("drive", "v3", credentials=creds)
            files = drive.files().list(
                q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
                fields="files(id, name)", pageSize=1000
            ).execute().get("files", [])
            if not files:
                st.error("No spreadsheets found in the folder.")
                st.stop()
            for f in files:
                url = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
                _, ws = open_ws_by_url(gc, url, source_tab)
                frames.append(read_df(ws))

        result_df = summarize_frames(frames)
        st.subheader("Preview Result")
        st.dataframe(result_df)

        dest_sheet_url = ensure_destination_sheet(gc, creds, dest_sheet_url)
        write_to_dest(gc, dest_sheet_url, dest_tab, result_df)
        st.success(f"✅ Wrote {len(result_df)} rows to {dest_tab} in {dest_sheet_url}")

    except Exception as e:
        st.exception(e)
