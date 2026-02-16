import asyncio
import logging
import os
import re
import sys

# --- FIX: FORCE EVENT LOOP FOR PYTHON 3.14+ ---
# This must run BEFORE 'from pyrogram import Client'
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from datetime import datetime, timedelta
from pyrogram import Client, filters
import motor.motor_asyncio
from aiohttp import web

# --- IMPORT YOUR ENGINE ---
try:
    from pahe_engine import PaheEngine
except ImportError:
    print("❌ CRITICAL: 'pahe_engine.py' not found! The bot needs this file to scrape anime.")
    sys.exit(1)

# --- CONFIGURATION (Load from Environment Variables) ---
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
TARGET_GROUP = int(os.getenv("TARGET_GROUP", "0")) 
MONGO_URL = os.getenv("MONGO_URL", "")
PORT = int(os.getenv("PORT", 8080)) 

# --- SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Controller")

if not MONGO_URL or not SESSION_STRING:
    logger.error("❌ Config Missing! Make sure MONGO_URL and SESSION_STRING are set in Render.")
    sys.exit(1)

mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = mongo["AnimePaheBot"]
col_queue = db["queue"]

app = Client("controller_user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
engine = PaheEngine()

# --- DUMMY WEB SERVER (Keep Render Alive) ---
async def web_server():
    async def handle(request):
        return web.Response(text="Controller Bot is Running!")

    server = web.Application()
    server.router.add_get("/", handle)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌍 Web server started on port {PORT}")

# --- HELPER: STRONG NORMALIZER ---
def normalize(text):
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', text).lower()

# --- HELPER: WATCHER ---
async def wait_for_trigger(trigger_text, start_time, timeout=3600, require_text_in_msg=None):
    logger.info(f"👀 Watching for: '{trigger_text}' (Start Time: {start_time.strftime('%H:%M:%S')})")
    loop_start = asyncio.get_event_loop().time()
    
    while True:
        if (asyncio.get_event_loop().time() - loop_start) > timeout:
            return False

        found = False
        try:
            # Check last 10 messages in the target group
            async for msg in app.get_chat_history(TARGET_GROUP, limit=10):
                if not msg.text: continue

                # 1. Check Trigger Word
                if trigger_text.lower() in msg.text.lower():
                    
                    # 2. Check Timestamp (Allow slight 5s variance)
                    msg_time = msg.edit_date or msg.date 
                    if msg_time > (start_time - timedelta(seconds=5)):
                        
                        # 3. Check Required Text (Normalized)
                        if require_text_in_msg:
                            clean_req = normalize(require_text_in_msg)
                            clean_msg = normalize(msg.text)
                            
                            if clean_req in clean_msg:
                                logger.info(f"✅ MATCH FOUND: {msg.text[:50]}...")
                                found = True
                                break
                        else:
                            found = True
                            break

        except Exception as e:
            logger.error(f"⚠️ Error reading history: {e}")
            await asyncio.sleep(5)
        
        if found: return True
        await asyncio.sleep(10) # Check every 10 seconds

# --- CACHE WARMER ---
async def warm_up():
    logger.info("🔥 Warming up cache...")
    try:
        await app.get_chat(TARGET_GROUP)
        logger.info("✅ Resolved Target Group ID.")
    except:
        logger.error("❌ CRITICAL: Target Group NOT found! Make sure the user account has joined the group.")

# --- TASKS ---

async def task_poller():
    logger.info("📡 Polling Task Started.")
    while True:
        try:
            releases = engine.get_latest_releases()
            if releases:
                for item in releases:
                    session = item["session"]
                    # If not exists in DB, add it
                    if not await col_queue.find_one({"_id": session}):
                        entry = {
                            "_id": session,
                            "title": item["title"],
                            "ep": item["ep"],
                            "time": item["time"],
                            "found_at": datetime.utcnow(),
                            "status": "pending"
                        }
                        await col_queue.insert_one(entry)
                        logger.info(f"🆕 Queued: {item['title']} - Ep {item['ep']}")
        except Exception as e:
            logger.error(f"Polling Error: {e}")
        await asyncio.sleep(600) # Check every 10 minutes

async def task_downloader():
    logger.info("⬇️ Downloader Worker Started.")
    while True:
        item = await col_queue.find_one({"status": "pending"})
        if not item:
            await asyncio.sleep(5)
            continue
            
        title = item["title"]
        ep = item["ep"]
        
        try:
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloading"}})
            logger.info(f"▶️ [DL START] {title} Ep {ep}")
            dl_start_time = datetime.utcnow()

            # New: /anime Name -e 1 -r all
            dl_cmd = f'/anime {title} -e {ep} -r all'
            
            await app.send_message(TARGET_GROUP, dl_cmd)

            # Wait for Koyeb Bot to say "All done"
            success = await wait_for_trigger(
                trigger_text="All done", 
                start_time=dl_start_time,
                timeout=7200 # 2 hours timeout
            )

            if success:
                logger.info(f"✅ [DL FINISH] {title}")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloaded"}})
            else:
                logger.warning(f"❌ [DL FAILED] Timeout: {title}")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "failed_dl"}})

        except Exception as e:
            logger.error(f"Downloader Error: {e}")
            await asyncio.sleep(30)

async def task_uploader():
    # Note: This task relies on your Koyeb bot uploading the file to the channel.
    # It just marks it as "uploading" then "done" if found.
    logger.info("⬆️ Uploader Worker Started (Tracking Only).")
    while True:
        item = await col_queue.find_one({"status": "downloaded"})
        if not item:
            await asyncio.sleep(5)
            continue

        try:
            # Since the Koyeb bot handles the upload automatically now,
            # we just mark it as done in our DB to keep things clean.
            logger.info(f"✅ [UP FINISH] {item['title']} (Handled by Koyeb)")
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "done"}})
            
        except Exception as e:
            logger.error(f"Uploader Error: {e}")
            await asyncio.sleep(30)

async def main():
    await app.start()
    await warm_up()
    # Run Web Server and Tasks concurrently
    await asyncio.gather(web_server(), task_poller(), task_downloader(), task_uploader())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
