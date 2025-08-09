# streamlit_app.py
# Accumulate 20+ Google Sheets with same template, sum matching rows, keep only A,B,C,D,J in result.

import json
import re
from datetime import timezone, timedelta, datetime
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

# ---- Helpers ----

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
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def to_number_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")

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

    # Convert numeric columns
    numeric_cols = []
    for c in all_df.columns:
        cand = to_number_series(all_df[c])
        if cand.notna().any():
            all_df[c] = cand
            numeric_cols.append(c)

    # Group by ALL non-numeric columns
    key_cols = [c for c in all_df.columns if c not in numeric_cols]
    agg_map = {c: "sum" for c in numeric_cols}
    out = all_df.groupby(key_cols, dropna=False).agg(agg_map).reset_index()

    return out

def write_to_dest(gc, dest_sheet_url: str, dest_tab: str, df: pd.DataFrame):
    sh = gc.open_by_url(dest_sheet_url)
    try:
        ws = sh.worksheet(dest_tab)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        rows = max(len(df) + 5, 100)
        cols = max(len(df.columns) + 2, 26)
        ws = sh.add_worksheet(title=dest_tab, rows=rows, cols=cols)
    set_with_dataframe(ws, df if not df.empty else pd.DataFrame(),
                       include_index=False, include_column_header=True)

def ensure_destination_sheet(creds, dest_sheet_url: str, fallback_name: str = "Accumulated Report") -> str:
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

def normalize_folder_id(s: str) -> str:
    if not s:
        return ""
    if "/folders/" in s:
        try:
            return s.split("/folders/")[1].split("/")[0].split("?")[0]
        except Exception:
            return s
    return s

def normalize_sheet_url(u: str) -> str:
    u = (u or "").strip()
    m = re.search(r"https?://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9\-_]+)", u)
    if not m:
        raise ValueError(f"Not a Google Sheets URL: {u}")
    return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/edit"

# ---- UI ----

st.set_page_config(page_title="Sheets Accumulator", page_icon="📊")
st.title("📊 Google Sheets Accumulator — Keep Only A,B,C,D,J")

with st.expander("1) Authentication", expanded=True):
    default_secret = st.secrets.get("SA_JSON", None)
    sa_source = st.radio("Provide Service Account JSON", ["Use st.secrets[\"SA_JSON\"]", "Paste JSON text"],
                         index=0 if default_secret else 1)
    if sa_source == "Paste JSON text":
        sa_json_text = st.text_area("Paste your service account JSON", height=160)
    else:
        sa_json_text = default_secret or ""

with st.expander("2) Sources", expanded=True):
    src_mode = st.radio("Source mode", ["Folder ID", "List of Sheet URLs"], index=0)
    if src_mode == "Folder ID":
        folder_id_input = st.text_input("Folder ID or URL")
        folder_id = normalize_folder_id(folder_id_input)
        sheet_urls = []
    else:
        urls_raw = st.text_area("One Google Sheet URL per line", height=120)
        cleaned_urls = []
        for u in [u.strip() for u in urls_raw.splitlines() if u.strip()]:
            try:
                cleaned_urls.append(normalize_sheet_url(u))
            except ValueError as ve:
                st.error(str(ve))
        sheet_urls = cleaned_urls
        folder_id = ""
    source_tab = st.text_input("Source Tab Name", value="RawData")

with st.expander("3) Destination", expanded=True):
    dest_sheet_url = st.text_input("Destination Spreadsheet URL (blank to auto-create)")
    dest_tab = st.text_input("Destination Tab Name", value="Report")

run_clicked = st.button("🚀 Run Accumulation")

if run_clicked:
    try:
        if not sa_json_text:
            st.error("Missing Service Account JSON")
            st.stop()
        if src_mode == "List of Sheet URLs" and not sheet_urls:
            st.error("No sheet URLs provided")
            st.stop()
        if src_mode == "Folder ID" and not folder_id:
            st.error("No folder ID provided")
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
                st.error("No spreadsheets found in folder")
                st.stop()
            for f in files:
                url = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
                _, ws = open_ws_by_url(gc, url, source_tab)
                frames.append(read_df(ws))

        # Summarize all sheets
        result_df = summarize_frames(frames)

        # Keep only columns A,B,C,D,J
        keep_cols = ["A", "B", "C", "D", "J"]
        result_df = result_df[[col for col in keep_cols if col in result_df.columns]]

        st.subheader("Preview Result")
        st.dataframe(result_df)
        st.write(f"Rows: {len(result_df)}")

        # Ensure destination sheet exists
        dest_sheet_url = ensure_destination_sheet(creds, dest_sheet_url)
        st.info(f"Destination: {dest_sheet_url}")

        # Write result
        write_to_dest(gc, dest_sheet_url, dest_tab, result_df)
        st.success(f"✅ Wrote {len(result_df)} rows to '{dest_tab}'")

    except Exception as e:
        st.exception(e)
