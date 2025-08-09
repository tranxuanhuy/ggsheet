# streamlit_app.py
# GUI to accumulate 20+ Google Sheets with the same template, filter by time window (Daily/Yearly/Custom),
# and write the summed result to a destination sheet.
#
# Deployment tips:
# - Put this file in a GitHub repo.
# - Create requirements.txt with (minimum):
#     streamlit
#     gspread
#     gspread-dataframe
#     pandas
#     google-auth
#     google-api-python-client
# - On Streamlit Community Cloud / HF Spaces: add a secret named SA_JSON containing your service-account JSON.

import json
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# Try to import googleapiclient; show a friendly UI hint if missing
try:
    from googleapiclient.discovery import build  # type: ignore
except Exception:
    build = None  # lazy fallback; we'll show guidance in the UI when needed

# ---- Helpers ---------------------------------------------------------------

def get_gc_and_creds(sa_json_text: str):
    """Return (gspread_client, google_credentials) using the same SA JSON."""
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
    # normalize columns (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def in_window(d: pd.Timestamp, start: datetime, end: datetime) -> bool:
    if pd.isna(d):
        return False
    return (d.to_pydatetime() >= start) and (d.to_pydatetime() < end)


def to_number_series(s: pd.Series) -> pd.Series:
    # Convert commas and strings to numeric where possible
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def summarize_frames(frames: List[pd.DataFrame], key_cols: List[str], date_col: str,
                      start: datetime, end: datetime) -> Tuple[pd.DataFrame, List[str]]:
    if not frames:
        return pd.DataFrame(), []

    # Validate headers consistent
    base_cols = list(frames[0].columns)
    for i, df in enumerate(frames[1:], start=2):
        if list(df.columns) != base_cols:
            raise ValueError(
                f"Sheet #{i} columns differ from template.\nExpected {base_cols}\nGot {list(df.columns)}"
            )

    # Check required cols
    for k in key_cols:
        if k not in base_cols:
            raise ValueError(f"KEY_COL '{k}' not found in columns: {base_cols}")
    if date_col not in base_cols:
        raise ValueError(f"DATE_COL '{date_col}' not found in columns: {base_cols}")

    # Filter by date window and stack
    all_df = pd.concat(frames, ignore_index=True)
    all_df[date_col] = pd.to_datetime(all_df[date_col], errors="coerce")
    mask = all_df[date_col].apply(lambda d: in_window(d, start, end))
    all_df = all_df[mask]

    if all_df.empty:
        return pd.DataFrame(columns=key_cols), base_cols

    # Identify numeric columns = non-key, non-date with any numeric values
    non_keys = [c for c in all_df.columns if c not in (set(key_cols) | {date_col})]
    numeric_cols = []
    for c in non_keys:
        cand = to_number_series(all_df[c])
        if cand.notna().any():
            all_df[c] = cand
            numeric_cols.append(c)

    agg_map = {c: "sum" for c in numeric_cols}
    out = (
        all_df.groupby(key_cols, dropna=False)
        .agg(agg_map)
        .reset_index()
    )

    # Reorder to match template: keys first, then numeric in their original order
    ordered = key_cols + [c for c in base_cols if c in numeric_cols and c not in key_cols]
    out = out.reindex(columns=ordered)
    return out, base_cols


def write_to_dest(gc, dest_sheet_url: str, dest_tab: str, df: pd.DataFrame):
    sh = gc.open_by_url(dest_sheet_url)
    try:
        ws = sh.worksheet(dest_tab)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        rows = max(len(df) + 5, 100)
        cols = max(len(df.columns) + 2, 26)
        ws = sh.add_worksheet(title=dest_tab, rows=rows, cols=cols)

    set_with_dataframe(ws, df if not df.empty else pd.DataFrame(), include_index=False, include_column_header=True)


def ensure_destination_sheet(gc, creds, dest_sheet_url: str, fallback_name: str = "Accumulated Report") -> str:
    """Return a valid destination spreadsheet URL. If empty, auto-create a new Google Sheet."""
    if dest_sheet_url:
        return dest_sheet_url
    if build is None:
        raise RuntimeError("google-api-python-client not installed. Add 'google-api-python-client' to requirements.txt and redeploy.")
    drive = build("drive", "v3", credentials=creds)
    file = drive.files().create(
        body={"name": fallback_name, "mimeType": "application/vnd.google-apps.spreadsheet"},
        fields="id, webViewLink"
    ).execute()
    new_url = f"https://docs.google.com/spreadsheets/d/{file['id']}/edit"
    return new_url


# ---- UI -------------------------------------------------------------------

st.set_page_config(page_title="Sheets Accumulator", page_icon="📊", layout="centered")
st.title("📊 Google Sheets Accumulator (Daily / Yearly / Custom)")

st.markdown(
    "This app sums numbers across many spreadsheets with the same template and writes the result to a destination sheet."
)

with st.expander("1) Authentication (Service Account JSON)", expanded=True):
    default_secret = st.secrets.get("SA_JSON", None)
    sa_source = st.radio("How will you provide Service Account JSON?", ["Use st.secrets[\"SA_JSON\"]", "Paste JSON text"],
                         index=0 if default_secret else 1)
    if sa_source == "Paste JSON text":
        sa_json_text = st.text_area("Paste your service account JSON here", height=160)
    else:
        if not default_secret:
            st.info("No SA_JSON found in secrets. Paste JSON instead.")
        sa_json_text = default_secret or ""

with st.expander("2) Sources", expanded=True):
    src_mode = st.radio("How do you want to specify sources?", ["Folder ID", "List of Sheet URLs"], index=0)
    if src_mode == "Folder ID":
        folder_id = st.text_input("Google Drive Folder ID", placeholder="e.g. 1AbCDEF...")
        st.caption("Put all source spreadsheets in this folder.")
        sheet_urls: List[str] = []
    else:
        urls_raw = st.text_area("One Google Sheet URL per line", height=120)
        sheet_urls = [u.strip() for u in urls_raw.splitlines() if u.strip()]
        folder_id = ""

    source_tab = st.text_input("Source Tab Name", value="RawData")

with st.expander("3) Destination", expanded=True):
    dest_sheet_url = st.text_input("Destination Spreadsheet URL (leave blank to auto-create)", placeholder="https://docs.google.com/spreadsheets/d/DEST_ID/edit")
    dest_tab_daily = st.text_input("Destination Tab (Daily)", value="Report_Daily")
    dest_tab_yearly = st.text_input("Destination Tab (Yearly)", value="Report_Yearly")

with st.expander("4) Template & Filters", expanded=True):
    # For discovering headers & picking key/date columns
    if st.button("🔍 Preview Headers from First Source"):
        try:
            if not sa_json_text:
                st.error("Provide Service Account JSON first.")
            else:
                gc, creds = get_gc_and_creds(sa_json_text)
                if src_mode == "List of Sheet URLs":
                    if not sheet_urls:
                        st.error("Enter at least one source URL.")
                    else:
                        _, ws = open_ws_by_url(gc, sheet_urls[0], source_tab)
                else:
                    if not folder_id:
                        st.error("Enter a Folder ID or switch to URLs mode.")
                    else:
                        if build is None:
                            st.error("google-api-python-client is not installed. Add 'google-api-python-client' to requirements.txt and redeploy.")
                        else:
                            # Pick first spreadsheet in folder
                            drive = build("drive", "v3", credentials=creds)
                            files = drive.files().list(
                                q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
                                fields="files(id, name)", pageSize=1
                            ).execute().get("files", [])
                            if not files:
                                st.error("No spreadsheets found in that folder.")
                            else:
                                first_url = f"https://docs.google.com/spreadsheets/d/{files[0]['id']}/edit"
                                _, ws = open_ws_by_url(gc, first_url, source_tab)
                if 'ws' in locals():
                    df0 = read_df(ws)
                    st.session_state['headers'] = list(df0.columns)
                    st.success("Loaded headers:")
                    st.code("\n".join(st.session_state['headers']))
        except Exception as e:
            st.exception(e)

    headers = st.session_state.get('headers', [])
    key_cols = st.multiselect("KEY columns (non-numeric identifiers)", options=headers, default=[h for h in headers[:2]])
    date_col = st.selectbox("Date column (for filtering)", options=headers if headers else ["Date"], index=0)

with st.expander("5) Time Window", expanded=True):
    mode = st.radio("Select mode", ["Daily (yesterday)", "Yearly (previous year)", "Custom range"], index=0)
    tz = timezone(timedelta(hours=7))  # Asia/Ho_Chi_Minh
    today_local = datetime.now(tz).date()

    if mode == "Daily (yesterday)":
        start_dt = datetime.combine(today_local - timedelta(days=1), datetime.min.time()).replace(tzinfo=tz)
        end_dt = datetime.combine(today_local, datetime.min.time()).replace(tzinfo=tz)
        st.info(f"Will aggregate for {start_dt.date()} (yesterday).")
    elif mode == "Yearly (previous year)":
        year = today_local.year - 1
        start_dt = datetime(year, 1, 1, tzinfo=tz)
        end_dt = datetime(year + 1, 1, 1, tzinfo=tz)
        st.info(f"Will aggregate for {year}-01-01 to {year}-12-31.")
    else:
        d_from = st.date_input("Start date", value=today_local - timedelta(days=7))
        d_to = st.date_input("End date (exclusive)", value=today_local + timedelta(days=1))
        start_dt = datetime.combine(d_from, datetime.min.time()).replace(tzinfo=tz)
        end_dt = datetime.combine(d_to, datetime.min.time()).replace(tzinfo=tz)

run_clicked = st.button("🚀 Run Accumulation")

if run_clicked:
    try:
        # Validate inputs
        if not sa_json_text:
            st.error("Missing Service Account JSON.")
            st.stop()
        if src_mode == "List of Sheet URLs" and not sheet_urls:
            st.error("Please enter at least one source sheet URL.")
            st.stop()
        if src_mode == "Folder ID" and not folder_id:
            st.error("Please enter a Drive Folder ID or switch to URLs mode.")
            st.stop()
        if not key_cols:
            st.error("Select at least one KEY column.")
            st.stop()

        gc, creds = get_gc_and_creds(sa_json_text)

        # Collect source dataframes
        frames: List[pd.DataFrame] = []
        if src_mode == "List of Sheet URLs":
            for u in sheet_urls:
                _, ws = open_ws_by_url(gc, u, source_tab)
                frames.append(read_df(ws))
        else:
            # Folder ID mode -> list spreadsheets via Drive API
            if build is None:
                st.error("google-api-python-client is not installed. Add 'google-api-python-client' to requirements.txt and redeploy.")
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

        # Summarize
        result_df, base_cols = summarize_frames(
            frames,
            key_cols,
            date_col,
            start_dt.astimezone(timezone.utc).replace(tzinfo=None),
            end_dt.astimezone(timezone.utc).replace(tzinfo=None),
        )

        st.subheader("Preview Result")
        st.dataframe(result_df.head(200))
        st.write(f"Rows: {len(result_df)} | Columns: {len(result_df.columns)}")

        # Ensure destination (auto-create if empty)
        dest_sheet_url = ensure_destination_sheet(gc, creds, dest_sheet_url)
        st.info(f"Destination: {dest_sheet_url}")

        # Write
        dest_tab = (
            dest_tab_daily if mode == "Daily (yesterday)" else (
                dest_tab_yearly if mode == "Yearly (previous year)" else "Report_Custom"
            )
        )
        write_to_dest(gc, dest_sheet_url, dest_tab, result_df)
        st.success(f"✅ Wrote {len(result_df)} rows to '{dest_tab}'.")
    except Exception as e:
        st.error("Failed to run accumulation. See details below.")
        st.exception(e)
