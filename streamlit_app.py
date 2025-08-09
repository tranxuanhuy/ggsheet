# streamlit_app.py
# Simple accumulator: read from row 5, keep columns A–J, no date/key filtering

import json
import re
from datetime import timezone
from typing import List

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

try:
    from googleapiclient.discovery import build
except Exception:
    build = None


# ----------------- HELPERS -----------------
def get_gc_and_creds(sa_json_text: str):
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
    """Read sheet with header at row 5 and keep columns A–J."""
    all_values = ws.get_all_values()
    if len(all_values) < 5:
        return pd.DataFrame()

    headers = all_values[4]  # Row 5 = index 4
    data_rows = all_values[5:]  # Data starts row 6
    df = pd.DataFrame(data_rows, columns=headers)
    df.columns = [str(c).strip() for c in df.columns]

    # Keep only first 10 columns (A–J)
    df = df.iloc[:, 0:10]
    return df


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


def ensure_destination_sheet(gc, creds, dest_sheet_url: str, fallback_name: str = "Accumulated Report") -> str:
    if dest_sheet_url:
        return dest_sheet_url
    if build is None:
        raise RuntimeError("google-api-python-client not installed.")
    drive = build("drive", "v3", credentials=creds)
    file = drive.files().create(
        body={"name": fallback_name, "mimeType": "application/vnd.google-apps.spreadsheet"},
        fields="id, webViewLink"
    ).execute()
    return f"https://docs.google.com/spreadsheets/d/{file['id']}/edit"


def normalize_folder_id(s: str) -> str:
    if not s:
        return ""
    if "/folders/" in s:
        return s.split("/folders/")[1].split("/")[0].split("?")[0]
    return s


def normalize_sheet_url(u: str) -> str:
    u = (u or "").strip()
    m = re.search(r"https?://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9\-_]+)", u)
    if not m:
        raise ValueError(f"Not a Google Sheets URL: {u}")
    return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/edit"


# ----------------- UI -----------------
st.set_page_config(page_title="Sheets Accumulator A–J", page_icon="📊")
st.title("📊 Google Sheets Accumulator (Row 5 header, A–J columns)")

with st.expander("1) Authentication", expanded=True):
    default_secret = st.secrets.get("SA_JSON", None)
    sa_source = st.radio("Provide Service Account JSON", ["Use st.secrets[\"SA_JSON\"]", "Paste JSON text"],
                         index=0 if default_secret else 1)
    if sa_source == "Paste JSON text":
        sa_json_text = st.text_area("Paste Service Account JSON", height=160)
    else:
        sa_json_text = default_secret or ""

with st.expander("2) Sources", expanded=True):
    src_mode = st.radio("Specify sources by", ["Folder ID", "List of Sheet URLs"], index=0)
    if src_mode == "Folder ID":
        folder_id_input = st.text_input("Google Drive Folder ID or URL")
        folder_id = normalize_folder_id(folder_id_input)
        sheet_urls = []
    else:
        urls_raw = st.text_area("One Google Sheet URL per line", height=120)
        cleaned_urls, bad = [], []
        for u in [u.strip() for u in urls_raw.splitlines() if u.strip()]:
            try:
                cleaned_urls.append(normalize_sheet_url(u))
            except ValueError as ve:
                bad.append(str(ve))
        if bad:
            st.error("Invalid URLs:\n" + "\n".join(bad))
        sheet_urls = cleaned_urls
        folder_id = ""

    source_tab = st.text_input("Source Tab Name", value="RawData")

with st.expander("3) Destination", expanded=True):
    dest_sheet_url = st.text_input("Destination Spreadsheet URL (leave blank to auto-create)")
    dest_tab = st.text_input("Destination Tab Name", value="Report_All")

if st.button("🚀 Run Accumulation"):
    try:
        if not sa_json_text:
            st.error("Missing Service Account JSON.")
            st.stop()
        if src_mode == "List of Sheet URLs" and not sheet_urls:
            st.error("Enter at least one valid Google Sheets URL.")
            st.stop()
        if src_mode == "Folder ID" and not folder_id:
            st.error("Enter a Folder ID or switch mode.")
            st.stop()

        gc, creds = get_gc_and_creds(sa_json_text)

        frames: List[pd.DataFrame] = []
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
                st.error("No spreadsheets found.")
                st.stop()
            for f in files:
                url = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
                _, ws = open_ws_by_url(gc, url, source_tab)
                frames.append(read_df(ws))

        if not frames:
            st.error("No data found.")
            st.stop()

        # Combine all data
        result_df = pd.concat(frames, ignore_index=True)

        st.subheader("Preview Result")
        st.dataframe(result_df.head(50))
        st.write(f"Rows: {len(result_df)}, Columns: {len(result_df.columns)}")

        dest_sheet_url = ensure_destination_sheet(gc, creds, dest_sheet_url)
        write_to_dest(gc, dest_sheet_url, dest_tab, result_df)
        st.success(f"✅ Wrote {len(result_df)} rows to {dest_tab} in {dest_sheet_url}")

    except Exception as e:
        st.error("Failed to run accumulation.")
        st.exception(e)
