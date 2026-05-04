import os
import json
import time
import requests
import pandas as pd
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

MAX_WORKERS = 5
OUTPUT_DIR = "output"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# ---------------- DATE ----------------

def get_date_range(days_back):
    if days_back == 0:
        start = end = date.today()
    elif days_back == 1:
        start = end = date.today() - timedelta(days=1)
    else:
        end = date.today()
        start = end - timedelta(days=days_back - 1)
    return start, end

# ---------------- FILE ----------------

def get_output_file(pipeline_name, start_date, end_date):
    return os.path.join(
        OUTPUT_DIR,
        f"{pipeline_name} - {end_date} to {start_date}.csv"
    )

# ---------------- FETCH: INSIGHTS ----------------

def fetch_insights(account_id, token, start_date, end_date):

    url = f"https://graph.facebook.com/v18.0/{account_id}/insights"

    params = {
        "access_token": token,
        "time_range": json.dumps({
            "since": str(start_date),
            "until": str(end_date)
        }),
        "fields": ",".join([
            "date_start","date_stop",
            "account_id","account_name",
            "campaign_id","campaign_name",
            "adset_id","adset_name",
            "ad_id","ad_name",
            "spend","impressions","reach",
            "ctr","cpc","cpp","frequency",
            "actions",
            "conversions",
            "cost_per_conversion",
            "cost_per_result",
            "results",
            "buying_type"
        ]),
        "level": "ad",
        "limit": 100
    }

    all_rows = []

    while url:
        res = requests.get(url, params=params)
        data = res.json()

        if "error" in data:
            print("❌ Insights error:", data["error"])
            break

        all_rows.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = None
        time.sleep(1)

    return all_rows

# ---------------- FETCH: ADSETS ----------------

def fetch_adsets(account_id, token):

    url = f"https://graph.facebook.com/v18.0/{account_id}/adsets"

    params = {
        "access_token": token,
        "fields": "id,daily_budget,bid_amount",
        "limit": 200
    }

    data_map = {}

    while url:
        data = requests.get(url, params=params).json()

        for a in data.get("data", []):
            data_map[a["id"]] = {
                "adset_daily_budget": a.get("daily_budget"),
                "bid_amount": a.get("bid_amount")
            }

        url = data.get("paging", {}).get("next")
        params = None

    return data_map

# ---------------- FETCH: CAMPAIGNS ----------------

def fetch_campaigns(account_id, token):

    url = f"https://graph.facebook.com/v18.0/{account_id}/campaigns"

    params = {
        "access_token": token,
        "fields": "id,daily_budget",
        "limit": 200
    }

    data_map = {}

    while url:
        data = requests.get(url, params=params).json()

        for c in data.get("data", []):
            data_map[c["id"]] = {
                "campaign_daily_budget": c.get("daily_budget")
            }

        url = data.get("paging", {}).get("next")
        params = None

    return data_map

# ---------------- FETCH: ADS ----------------

def fetch_ads(account_id, token):

    url = f"https://graph.facebook.com/v18.0/{account_id}/ads"

    params = {
        "access_token": token,
        "fields": "id,status,effective_status",
        "limit": 200
    }

    data_map = {}

    while url:
        data = requests.get(url, params=params).json()

        for a in data.get("data", []):
            data_map[a["id"]] = {
                "ad_status": a.get("status"),
                "ad_effective_status": a.get("effective_status")
            }

        url = data.get("paging", {}).get("next")
        params = None

    return data_map

# ---------------- MERGE ----------------

def merge_data(insights, adsets, campaigns, ads):

    final = []

    for r in insights:

        adset = adsets.get(r.get("adset_id"), {})
        campaign = campaigns.get(r.get("campaign_id"), {})
        ad = ads.get(r.get("ad_id"), {})

        final.append({
            "date_start": r.get("date_start"),
            "date_stop": r.get("date_stop"),
            "account_id": r.get("account_id"),
            "account_name": r.get("account_name"),
            "campaign_id": r.get("campaign_id"),
            "campaign_name": r.get("campaign_name"),
            "adset_id": r.get("adset_id"),
            "adset_name": r.get("adset_name"),
            "ad_id": r.get("ad_id"),
            "ad_name": r.get("ad_name"),
            "spend": r.get("spend"),
            "impressions": r.get("impressions"),
            "reach": r.get("reach"),
            "ctr": r.get("ctr"),
            "cpc": r.get("cpc"),
            "cpp": r.get("cpp"),
            "frequency": r.get("frequency"),
            "buying_type": r.get("buying_type"),

            # 🔥 RAW ARRAYS (UNCHANGED)
            "actions": json.dumps(r.get("actions", [])),
            "conversions": json.dumps(r.get("conversions", [])),
            "cost_per_conversion": json.dumps(r.get("cost_per_conversion", [])),
            "cost_per_result": json.dumps(r.get("cost_per_result", [])),
            "results": json.dumps(r.get("results", [])),

            # budgets + bids
            "adset_daily_budget": adset.get("adset_daily_budget"),
            "bid_amount": adset.get("bid_amount"),
            "campaign_daily_budget": campaign.get("campaign_daily_budget"),

            # status
            "ad_status": ad.get("ad_status"),
            "ad_effective_status": ad.get("ad_effective_status")
        })

    return final

# ---------------- PIPELINE ----------------

def run_pipeline_for_accounts(accounts, token, days_back, pipeline_name, progress_callback=None):

    start_date, end_date = get_date_range(days_back)
    output_file = get_output_file(pipeline_name, start_date, end_date)

    if os.path.exists(output_file):
        os.remove(output_file)

    for i, acc in enumerate(accounts, 1):

        if progress_callback:
            progress_callback({
                "pipeline": pipeline_name,
                "current": i,
                "total": len(accounts),
                "account": acc
            })

        insights = fetch_insights(acc, token, start_date, end_date)
        adsets = fetch_adsets(acc, token)
        campaigns = fetch_campaigns(acc, token)
        ads = fetch_ads(acc, token)

        final_rows = merge_data(insights, adsets, campaigns, ads)

        if final_rows:
            df = pd.DataFrame(final_rows)

            if not os.path.exists(output_file):
                df.to_csv(output_file, index=False)
            else:
                df.to_csv(output_file, mode="a", header=False, index=False)

# ---------------- SINGLE ----------------

def run_single_pipeline(pipeline_name, days_back, progress_callback=None):

    pipelines = json.load(open("pipelines.json"))

    p = next((x for x in pipelines if x["name"] == pipeline_name), None)

    if not p:
        print("❌ Pipeline not found")
        return

    run_pipeline_for_accounts(
        p["accounts"],
        p["access_token"],
        days_back,
        p["name"],
        progress_callback
    )

# ---------------- ALL ----------------

def run_all_pipelines(days_back, progress_callback=None):

    pipelines = json.load(open("pipelines.json"))

    def run(p):
        run_pipeline_for_accounts(
            p["accounts"],
            p["access_token"],
            days_back,
            p["name"],
            progress_callback
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(run, pipelines)