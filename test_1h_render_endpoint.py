import requests
import time
import json
import sys

# --- Configuration ---
# Your Render server URL
BASE_URL = "https://bazzar-kline-data-fetcher.onrender.com"

# Your Secret Token
SECRET_TOKEN = "O0hrTGEd3meImdof/H0Hj2XOKuVgQAbr+D9w0DRZvtA="

# 1. Endpoints from your Python project
ENDPOINT_1H_TASK = "/internal/update-1h-and-check-alerts"
ENDPOINT_1H_CACHE = "/get-cache/1h"

# 2. Wait time
WAIT_TIME_SECONDS = 120  # 2 minutes

# 3. Headers for the secure endpoint
HEADERS = {
    "Authorization": f"Bearer {SECRET_TOKEN}",
    "Content-Type": "application/json",
}


def run_job():
    # ---------------------------------------------------------------
    # 1. "JERK" (TRIGGER) THE 1H ENDPOINT
    # ---------------------------------------------------------------
    try:
        jerk_url = BASE_URL + ENDPOINT_1H_TASK
        print(f"--- 1. Triggering 1h Job ---\nPOST {jerk_url}")
        
        # We add a timeout (e.g., 60 seconds) for the request itself
        response = requests.post(jerk_url, headers=HEADERS, timeout=60)
        
        if response.status_code == 200:
            print("✅ OK: 1h task completed successfully (200).")
            try:
                print(json.dumps(response.json(), indent=2))
            except requests.exceptions.JSONDecodeError:
                print(f"Response: {response.text}")
        elif response.status_code == 409:
            print("⚠️  OK: Task was already running (409 Conflict).")
        else:
            print(f"❌ ERROR: Failed to trigger job. Status: {response.status_code}")
            print(response.text)
            # We don't stop, we'll still try to get the cache

    except requests.exceptions.Timeout:
        print("❌ ERROR: The server took too long to respond to the trigger.")
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR during trigger: {e}")
        # We continue to the next steps anyway
    
    # ---------------------------------------------------------------
    # 2. WAIT FOR 2 MINUTES
    # ---------------------------------------------------------------
    print(f"\n--- 2. Waiting for {WAIT_TIME_SECONDS} seconds (2 minutes) ---")
    try:
        for i in range(WAIT_TIME_SECONDS):
            # Print a simple countdown
            print(f"Waiting... {WAIT_TIME_SECONDS - i} seconds remaining", end="\r", flush=True)
            time.sleep(1)
        print("\n✅ Wait complete.                                  ") # Extra spaces to clear line
    except KeyboardInterrupt:
        print("\n- Wait interrupted by user. Fetching cache now... -")
        

    # ---------------------------------------------------------------
    # 3. GET THE 1H CACHE
    # ---------------------------------------------------------------
    print(f"\n--- 3. Fetching 1h Cache ---")
    try:
        cache_url = BASE_URL + ENDPOINT_1H_CACHE
        print(f"GET {cache_url}")
        
        response = requests.get(cache_url, timeout=30)
        
        if response.status_code == 200:
            print("✅ OK: Successfully fetched 1h cache (200).")
            
            try:
                # Try to parse and show info
                data = response.json()
                print("\n--- Cache Data ---")
                
                audit = data.get('audit', {})
                print(f"Timeframe: {data.get('timeframe')}")
                print(f"Data for {audit.get('symbols_in_final_list')} coins")
                print(f"Cache Timestamp: {audit.get('timestamp')}")
                
                # Pretty print a small part
                if 'data' in data and data['data']:
                    print("\n--- Example Coin Data (first coin) ---")
                    # Set ensure_ascii=False to correctly print Russian text if any
                    print(json.dumps(data['data'][0], indent=2, ensure_ascii=False))
                    
            except requests.exceptions.JSONDecodeError:
                print("❌ ERROR: Received 200 OK, but response is not valid JSON.")
                print("Raw response (first 200 chars):", response.text[:200] + "...")
        
        else:
            print(f"❌ ERROR: Failed to get cache. Status: {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR during cache fetch: {e}")

    print("\n--- Script Finished ---")


if __name__ == "__main__":
    run_job()