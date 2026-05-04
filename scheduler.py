import time
import json
from datetime import datetime
from pipeline_runner import run_single_pipeline

SCHEDULE_FILE = "schedule.json"

def load_schedule():
    try:
        return json.load(open(SCHEDULE_FILE))
    except:
        return []

def scheduler_loop():

    print("Scheduler started...")

    while True:
        now = datetime.now().strftime("%H:%M")

        schedules = load_schedule()

        for s in schedules:
            if s["time"] == now:
                print(f"Running {s['pipeline']}")
                run_single_pipeline(s["pipeline"], s["days_back"])
                time.sleep(60)

        time.sleep(20)