import os
import time
import requests
from playwright.sync_api import sync_playwright
from apscheduler.schedulers.background import BackgroundScheduler

# Get the port Render assigned, or default to 10000
PORT = os.environ.get("PORT", 10000)
URL = f"http://127.0.0.1:{PORT}"

# Telegram Settings - We will securely inject these from Render later
TELEGRAM_TOKEN = os.environ.get("8605909436:AAHDDLQnVEEzs2pj1fxOxNRllcpHSCwYUos", "")
TELEGRAM_CHAT_ID = os.environ.get("1592988014", "")

def send_to_telegram(image_path, index_name):
    """Fires the screenshot directly to your Telegram App"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return # Skip if Telegram isn't configured
        
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    try:
        with open(image_path, "rb") as image_file:
            files = {"photo": image_file}
            data = {"chat_id": TELEGRAM_CHAT_ID, "caption": f"📊 {index_name} Update"}
            response = requests.post(url, data=data, files=files)
            
            if response.status_code == 200:
                print(f"✈️ Successfully sent {index_name} to Telegram!")
            else:
                print(f"❌ Telegram Error: {response.text}")
    except Exception as e:
        print(f"❌ Failed to send to Telegram: {e}")

def take_screenshots():
    print("📸 [AutoSnap] Waking up headless browser...")
    os.makedirs("static/screenshots", exist_ok=True)
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1920, "height": 1080})
            
            print(f"📸 [AutoSnap] Loading {URL}...")
            page.goto(URL, wait_until="networkidle")
            
            # Wait for data to load, plus 2 seconds for heatmaps to paint
            page.wait_for_function("document.getElementById('state-val').innerText !== '—'", timeout=30000)
            time.sleep(2) 
            
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
            
            # --- 1. SENSEX --- (Snapping this first as it's the active tab on your screenshot)
            page.click("#tab-SENSEX")
            time.sleep(1.5)
            sensex_path = f"static/screenshots/SENSEX_{timestamp}.png"
            page.screenshot(path=sensex_path, full_page=True)
            send_to_telegram(sensex_path, f"SENSEX ({time.strftime('%I:%M %p')})")
            
            # --- 2. NIFTY ---
            page.click("#tab-NIFTY")
            time.sleep(1.5)
            nifty_path = f"static/screenshots/NIFTY_{timestamp}.png"
            page.screenshot(path=nifty_path, full_page=True)
            send_to_telegram(nifty_path, f"NIFTY 50 ({time.strftime('%I:%M %p')})")
            
            # --- 3. BANKNIFTY ---
            page.click("#tab-BANKNIFTY")
            time.sleep(1.5)
            bank_path = f"static/screenshots/BANKNIFTY_{timestamp}.png"
            page.screenshot(path=bank_path, full_page=True)
            send_to_telegram(bank_path, f"BANK NIFTY ({time.strftime('%I:%M %p')})")
            
            browser.close()
            print("📸 [AutoSnap] Cycle complete. Sleeping for 5 minutes.")
            
    except Exception as e:
        print(f"❌ [AutoSnap] Failed: {e}")

def start_auto_snapper():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=take_screenshots, trigger="interval", minutes=5)
    scheduler.start()
    print("⏰ [AutoSnap] Background scheduler started.")