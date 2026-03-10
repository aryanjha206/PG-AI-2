"""
PG AI Query Engine — Application Entrypoint

Use:
    python app.py
"""

import subprocess
import sys
import webbrowser
import time
from urllib.error import URLError
from urllib.request import urlopen

API_URL = "http://127.0.0.1:8000"


def api_is_up(timeout: float = 1.0) -> bool:
    try:
        with urlopen(f"{API_URL}/get-database-tables", timeout=timeout) as resp:
            return resp.status == 200
    except (URLError, Exception):
        return False


def main():
    if api_is_up():
        print(f"[PG AI] Server already running at {API_URL}")
        webbrowser.open(API_URL)
        return

    print("[PG AI] Starting server...")
    proc = subprocess.Popen(
        [sys.executable, "api.py"],
        cwd=str(__import__("pathlib").Path(__file__).resolve().parent),
    )

    for _ in range(30):
        if api_is_up():
            break
        time.sleep(0.5)

    print(f"[PG AI] Server running at {API_URL}")
    webbrowser.open(API_URL)

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        print("\n[PG AI] Server stopped.")


if __name__ == "__main__":
    main()
