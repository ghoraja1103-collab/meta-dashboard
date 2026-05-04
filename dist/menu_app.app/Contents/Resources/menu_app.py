import rumps
import subprocess
import webbrowser
import time
import os
import psutil

# 🔧 UPDATE THESE PATHS (VERY IMPORTANT)
BASE_DIR = "/Users/rajatghosh/python/meta-dashboard"
VENV_PYTHON = "/Users/rajatghosh/python/meta-dashboard/venv/bin/python"

PORT = 8501


# ---------------- HELPERS ----------------

def kill_streamlit():
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            if proc.info['cmdline'] and "streamlit" in " ".join(proc.info['cmdline']):
                proc.kill()
        except:
            pass


def is_streamlit_running():
    for proc in psutil.process_iter(['cmdline']):
        try:
            if proc.info['cmdline'] and "streamlit" in " ".join(proc.info['cmdline']):
                return True
        except:
            pass
    return False


def start_streamlit():
    os.chdir(BASE_DIR)

    return subprocess.Popen(
        [
            VENV_PYTHON,
            "-m",
            "streamlit",
            "run",
            "dashboard.py",
            "--server.port=8501",
            "--server.address=127.0.0.1",
            "--server.headless=true"
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


# ---------------- APP ----------------

class MetaDashboardApp(rumps.App):

    def __init__(self):
        super(MetaDashboardApp, self).__init__("📊 Meta")

    @rumps.clicked("Open Dashboard")
    def open_dashboard(self, _):

        # kill old process first (important)
        kill_streamlit()

        # start fresh
        process = start_streamlit()

        # wait for server
        time.sleep(5)

        # open browser
        webbrowser.open(f"http://127.0.0.1:{PORT}")

    @rumps.clicked("Stop Dashboard")
    def stop_dashboard(self, _):
        kill_streamlit()

    @rumps.clicked("Quit")
    def quit_app(self, _):
        kill_streamlit()
        rumps.quit_application()


# ---------------- RUN ----------------

if __name__ == "__main__":
    MetaDashboardApp().run()