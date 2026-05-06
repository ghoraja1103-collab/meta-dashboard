import streamlit as st
import json, os, pandas as pd, requests, threading, signal, numpy as np
import gspread

from google.oauth2.service_account import Credentials

from scheduler import scheduler_loop
from pipeline_runner import run_single_pipeline, run_all_pipelines

PIPELINE_FILE = "pipelines.json"
OUTPUT_DIR = "output"
SCHEDULE_FILE = "schedule.json"

# ---------------- START SCHEDULER ----------------
if "scheduler_started" not in st.session_state:
    threading.Thread(target=scheduler_loop, daemon=True).start()
    st.session_state["scheduler_started"] = True

# ---------------- UTIL ----------------
def load(file):
    return json.load(open(file)) if os.path.exists(file) else []

def save(file, data):
    json.dump(data, open(file, "w"), indent=2)

# ---------------- GOOGLE SHEETS EXPORT ----------------
def export_to_gsheet_append(df, sheet_url, sheet_name):

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna("")
    df = df.astype(str)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = dict(st.secrets["gcp_service_account"])

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=scope
    )

    client = gspread.authorize(creds)

    spreadsheet = client.open_by_url(sheet_url)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows="1000",
            cols="50"
        )

    existing = worksheet.get_all_values()

    if not existing:
        worksheet.append_rows(
            [df.columns.tolist()] + df.values.tolist()
        )
    else:
        worksheet.append_rows(df.values.tolist())

# ---------------- FETCH ACCOUNTS ----------------
@st.cache_data(ttl=3600)
def get_accounts(token):

    url = "https://graph.facebook.com/v18.0/me/adaccounts"

    params = {
        "access_token": token,
        "fields": "id,name,account_id",
        "limit": 200
    }

    acc = []

    while url:
        d = requests.get(url, params=params).json()

        acc += d.get("data", [])

        url = d.get("paging", {}).get("next")
        params = None

    return acc

# ---------------- STATE ----------------
if "progress" not in st.session_state:
    st.session_state["progress"] = {}

st.title("Meta Dashboard")

# ---------------- STOP ----------------
if st.button("Stop Dashboard"):
    os.kill(os.getpid(), signal.SIGTERM)

# ---------------- CREATE PIPELINE ----------------
st.header("Create Pipeline")

name = st.text_input("Pipeline Name")
token = st.text_input("Token", type="password")

if st.button("Fetch Accounts"):
    st.session_state["accounts"] = get_accounts(token)

accounts = st.session_state.get("accounts", [])

options = {
    f"{a['name']} ({a['account_id']})": a["id"]
    for a in accounts
}

selected = st.multiselect(
    "Accounts",
    list(options.keys())
)

selected_ids = [options[s] for s in selected]

if st.button("Save Pipeline"):

    p = load(PIPELINE_FILE)

    p = [x for x in p if x["name"] != name]

    p.append({
        "name": name,
        "access_token": token,
        "accounts": selected_ids
    })

    save(PIPELINE_FILE, p)

    st.success("Saved")

# ---------------- RUN ----------------
pipelines = load(PIPELINE_FILE)

names = [p["name"] for p in pipelines]

selected_pipeline = st.selectbox(
    "Pipeline",
    names
)

range_opt = st.selectbox(
    "Range",
    ["Today", "Yesterday", "5", "7", "10"]
)

def mapd(x):
    return {
        "Today": 0,
        "Yesterday": 1,
        "5": 5,
        "7": 7,
        "10": 10
    }[x]

def progress(d):
    st.session_state["progress"] = d

col1, col2 = st.columns(2)

with col1:
    if st.button("Run Selected"):
        run_single_pipeline(
            selected_pipeline,
            mapd(range_opt),
            progress
        )

with col2:
    if st.button("Run ALL"):
        run_all_pipelines(
            mapd(range_opt),
            progress
        )

# ---------------- PROGRESS ----------------
p = st.session_state["progress"]

if p:
    st.write(p)

    st.progress(
        p["current"] / max(p["total"], 1)
    )

# ---------------- FILES ----------------
st.header("Files")

files = os.listdir(OUTPUT_DIR) if os.path.exists(OUTPUT_DIR) else []

for f in sorted(files, reverse=True):
    st.write(f)

# ---------------- PREVIEW ----------------
st.header("Preview")

if files:

    file = st.selectbox(
        "Select File",
        files
    )

    df = pd.read_csv(
        os.path.join(OUTPUT_DIR, file),
        dtype=str
    )

    st.write(f"Total Rows: {len(df)}")

    st.dataframe(
        df,
        use_container_width=True,
        height=400
    )

    # ---------------- DOWNLOAD ----------------
    st.subheader("Download CSV")

    csv_data = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download CSV File",
        data=csv_data,
        file_name=file,
        mime="text/csv"
    )

    # ---------------- GOOGLE SHEETS ----------------
    st.subheader("Export to Google Sheets")

    sheet_url = st.text_input("Google Sheet URL")

    sheet_name = st.text_input("Sheet Name")

    if st.button("Export to Sheets"):

        try:
            export_to_gsheet_append(
                df,
                sheet_url,
                sheet_name
            )

            st.success("Data exported")

        except Exception as e:
            st.error(str(e))