import os
import time
import requests
from playwright.sync_api import sync_playwright
from apscheduler.schedulers.background import BackgroundScheduler

# 🚨 SCREENSHOT BOT CREDENTIALS 🚨
SNAP_BOT_TOKEN = "8605909436:AAHDDLQnVEEzs2pj1fxOxNRllcpHSCwYUos"
SNAP_CHAT_ID   = "1592988014"

URL = "https://nifty-oi.onrender.com"

_browser_installed = False

def send_to_telegram(image_path, index_name):
    url = f"https://api.telegram.org/bot{SNAP_BOT_TOKEN}/sendPhoto"
    try:
        with open(image_path, "rb") as image_file:
            files = {"photo": image_file}
            data = {"chat_id": SNAP_CHAT_ID, "caption": f"📸 {index_name} Full Dashboard Snapshot"}
            response = requests.post(url, data=data, files=files)
            if response.status_code == 200:
                print(f"✈️ Successfully sent {index_name} screenshot to Telegram!")
            else:
                print(f"❌ Telegram Error: {response.text}")
    except Exception as e:
        print(f"❌ Failed to send screenshot: {e}")

def take_screenshots():
    global _browser_installed
    print("📸 [AutoSnap] Waking up headless browser...")
    os.makedirs("static/screenshots", exist_ok=True)
    
    if not _browser_installed:
        os.system("playwright install chromium")
        _browser_installed = True
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            page = browser.new_page(viewport={"width": 1920, "height": 1080})
            
            print(f"📸 [AutoSnap] Loading {URL}...")
            # Relaxed the networkidle requirement so it doesn't timeout if an API call hangs
            page.goto(URL, timeout=90000)
            
            # Wait for data to load
            page.wait_for_function("document.getElementById('state-val').innerText !== '—'", timeout=60000)
            time.sleep(4) 
            
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
            
            # 1. NIFTY
            page.click("#tab-NIFTY")
            time.sleep(2)
            nifty_path = f"static/screenshots/NIFTY_{timestamp}.png"
            page.screenshot(path=nifty_path, full_page=True)
            send_to_telegram(nifty_path, "NIFTY 50")
            
            # 2. BANKNIFTY
            page.click("#tab-BANKNIFTY")
            time.sleep(2)
            bank_path = f"static/screenshots/BANKNIFTY_{timestamp}.png"
            page.screenshot(path=bank_path, full_page=True)
            send_to_telegram(bank_path, "BANK NIFTY")
            
            # 3. SENSEX
            page.click("#tab-SENSEX")
            time.sleep(2)
            sensex_path = f"static/screenshots/SENSEX_{timestamp}.png"
            page.screenshot(path=sensex_path, full_page=True)
            send_to_telegram(sensex_path, "SENSEX")
            
            browser.close()
            print("📸 [AutoSnap] Cycle complete. Sleeping for 5 minutes.")
            
    except Exception as e:
        print(f"❌ [AutoSnap] Failed: {e}")

def start_auto_snapper():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=take_screenshots, trigger="interval", minutes=5)
    scheduler.start()
    print("⏰ [AutoSnap] Background scheduler started.")