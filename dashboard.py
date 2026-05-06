import streamlit as st
import json
import os
import pandas as pd
import requests
import threading
import signal
import numpy as np
import gspread

from google.oauth2.service_account import Credentials

from scheduler import scheduler_loop
from pipeline_runner import run_single_pipeline, run_all_pipelines

PIPELINE_FILE = "pipelines.json"

# ---------------- START SCHEDULER ----------------
if "scheduler_started" not in st.session_state:
    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()

    st.session_state["scheduler_started"] = True

# ---------------- UTIL ----------------
def load(file):

    if os.path.exists(file):

        with open(file, "r") as f:
            return json.load(f)

    return []

def save(file, data):

    with open(file, "w") as f:
        json.dump(data, f, indent=2)

# ---------------- GOOGLE SHEETS EXPORT ----------------
def export_to_gsheet_append(
    df,
    sheet_url,
    sheet_name
):

    df = df.replace([np.inf, -np.inf], np.nan)

    df = df.fillna("")

    df = df.astype(str)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = dict(
        st.secrets["gcp_service_account"]
    )

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=scope
    )

    client = gspread.authorize(creds)

    spreadsheet = client.open_by_url(sheet_url)

    try:

        worksheet = spreadsheet.worksheet(
            sheet_name
        )

    except:

        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows="1000",
            cols="50"
        )

    existing = worksheet.get_all_values()

    if not existing:

        worksheet.append_rows(
            [df.columns.tolist()] +
            df.values.tolist()
        )

    else:

        worksheet.append_rows(
            df.values.tolist()
        )

# ---------------- FETCH ACCOUNTS ----------------
@st.cache_data(ttl=3600)
def get_accounts(token):

    url = "https://graph.facebook.com/v18.0/me/adaccounts"

    params = {
        "access_token": token,
        "fields": "id,name,account_id",
        "limit": 500
    }

    acc = []

    while url:

        d = requests.get(
            url,
            params=params
        ).json()

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

token = st.text_input(
    "Meta Token",
    type="password"
)

if st.button("Fetch Accounts"):

    accounts_data = get_accounts(token)

    st.session_state["accounts"] = accounts_data

    st.success(
        f"Fetched {len(accounts_data)} accounts"
    )

accounts = st.session_state.get(
    "accounts",
    []
)

options = {
    f"{a['name']} ({a['account_id']})": a["id"]
    for a in accounts
}

selected = st.multiselect(
    "Accounts",
    list(options.keys())
)

selected_ids = [
    options[s]
    for s in selected
]

if st.button("Save Pipeline"):

    pipelines = load(PIPELINE_FILE)

    pipelines = [
        p for p in pipelines
        if p["name"] != name
    ]

    pipelines.append({
        "name": name,
        "access_token": token,
        "accounts": selected_ids
    })

    save(PIPELINE_FILE, pipelines)

    st.success("Pipeline saved")

# ---------------- MANAGE PIPELINES ----------------
st.header("Manage Pipelines")

pipelines = load(PIPELINE_FILE)

if pipelines:

    pipeline_names = [
        p["name"]
        for p in pipelines
    ]

    selected_manage = st.selectbox(
        "Select Pipeline",
        pipeline_names,
        key="manage_pipeline"
    )

    selected_pipeline_data = next(
        p for p in pipelines
        if p["name"] == selected_manage
    )

    st.write(
        f"Accounts: {len(selected_pipeline_data['accounts'])}"
    )

    new_name = st.text_input(
        "Edit Pipeline Name",
        value=selected_pipeline_data["name"]
    )

    col1, col2 = st.columns(2)

    with col1:

        if st.button("Update Pipeline"):

            selected_pipeline_data["name"] = new_name

            save(
                PIPELINE_FILE,
                pipelines
            )

            st.success("Pipeline updated")

    with col2:

        if st.button("Delete Pipeline"):

            pipelines = [
                p for p in pipelines
                if p["name"] != selected_manage
            ]

            save(
                PIPELINE_FILE,
                pipelines
            )

            st.success("Pipeline deleted")

            st.rerun()

# ---------------- RUN PIPELINES ----------------
st.header("Run Pipelines")

pipelines = load(PIPELINE_FILE)

names = [
    p["name"]
    for p in pipelines
]

selected_pipeline = st.selectbox(
    "Pipeline",
    names,
    key="run_pipeline"
)

range_opt = st.selectbox(
    "Range",
    [
        "Today",
        "Yesterday",
        "5",
        "7",
        "10"
    ]
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

        df = run_single_pipeline(
            selected_pipeline,
            mapd(range_opt),
            progress
        )

        if df is not None:
            st.session_state["latest_df"] = df

with col2:

    if st.button("Run ALL"):

        df = run_all_pipelines(
            mapd(range_opt),
            progress
        )

        if df is not None:
            st.session_state["latest_df"] = df

# ---------------- PROGRESS ----------------
p = st.session_state["progress"]

if p:

    st.write(p)

    st.progress(
        p["current"] / max(p["total"], 1)
    )

# ---------------- PREVIEW ----------------
st.header("Preview")

if "latest_df" in st.session_state:

    df = st.session_state["latest_df"]

    st.write(
        f"Total Rows: {len(df)}"
    )

    st.dataframe(
        df,
        use_container_width=True,
        height=500
    )

    # ---------------- DOWNLOAD CSV ----------------
    st.subheader("Download CSV")

    csv_data = df.to_csv(
        index=False
    ).encode("utf-8")

    st.download_button(
        label="Download CSV File",
        data=csv_data,
        file_name="meta_data.csv",
        mime="text/csv"
    )

    # ---------------- EXPORT TO SHEETS ----------------
    st.subheader("Export to Google Sheets")

    sheet_url = st.text_input(
        "Google Sheet URL"
    )

    sheet_name = st.text_input(
        "Sheet Name"
    )

    if st.button("Export to Sheets"):

        try:

            export_to_gsheet_append(
                df,
                sheet_url,
                sheet_name
            )

            st.success(
                "Data exported"
            )

        except Exception as e:

            st.error(str(e))