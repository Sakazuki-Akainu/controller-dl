import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pyrogram import Client, filters
import motor.motor_asyncio
from aiohttp import web

# --- IMPORT YOUR ENGINE ---
try:
    from pahe_engine import PaheEngine
except ImportError:
    print("❌ CRITICAL: 'pahe_engine.py' not found!")
    sys.exit(1)

# --- CONFIGURATION ---
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
    logger.error("❌ Config Missing!")
    sys.exit(1)

mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = mongo["AnimePaheBot"]
col_queue = db["queue"]

app = Client("controller_user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
engine = PaheEngine()

# --- DUMMY WEB SERVER ---
async def web_server():
    async def handle(request): return web.Response(text="Controller Bot is Running!")
    server = web.Application()
    server.router.add_get("/", handle)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌍 Web server started on port {PORT}")

# --- HELPER: NORMALIZER ---
def normalize(text):
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', text).lower()

# --- HELPER: WATCHER (Detects Edited Messages) ---
async def wait_for_trigger(trigger_text, start_time, timeout=7200):
    logger.info(f"👀 Watching for: '{trigger_text}'...")
    loop_start = asyncio.get_event_loop().time()
    
    while True:
        if (asyncio.get_event_loop().time() - loop_start) > timeout:
            return False

        try:
            # Check last 5 messages (Koyeb bot usually replies recently)
            async for msg in app.get_chat_history(TARGET_GROUP, limit=5):
                if not msg.text: continue

                # Check if the message contains our trigger
                if trigger_text.lower() in msg.text.lower():
                    # Check Timestamp (Creation OR Edit time)
                    msg_time = msg.edit_date or msg.date
                    
                    # If the message was created OR edited AFTER we started waiting
                    if msg_time > (start_time - timedelta(seconds=10)):
                        logger.info(f"✅ Trigger Found: {msg.text[:30]}...")
                        return True

        except Exception as e:
            logger.error(f"⚠️ Watcher Error: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(10) # Check every 10 seconds

# --- CACHE WARMER ---
async def warm_up():
    try: await app.get_chat(TARGET_GROUP)
    except: logger.error("❌ Target Group NOT found!")

# --- DATABASE COMMANDS ---
@app.on_message(filters.command("queue"))
async def queue_status(client, message):
    try:
        # Get all pending items, sorted by time
        cursor = col_queue.find({"status": "pending"}).sort("found_at", 1)
        items = await cursor.to_list(length=20) # Limit to 20 to avoid spam
        
        if not items:
            await message.reply("✅ **Queue is empty!** No pending anime.")
            return

        text = "📋 **Pending Anime Queue:**\n\n"
        for i, item in enumerate(items, 1):
            text += f"**{i}.** {item['title']} - Ep {item['ep']} `[Pending]`\n"
        
        total = await col_queue.count_documents({"status": "pending"})
        if total > 20:
            text += f"\n...and {total - 20} more."

        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error fetching queue: {e}")

@app.on_message(filters.command("retry_all"))
async def retry_stuck(client, message):
    try:
        r = await col_queue.update_many({"status": "downloading"}, {"$set": {"status": "pending"}})
        await message.reply(f"🔄 Reset {r.modified_count} stuck items to Pending.")
    except: pass

# --- TASKS ---

async def task_poller():
    logger.info("📡 Polling Task Started.")
    while True:
        try:
            releases = engine.get_latest_releases()
            if releases:
                for item in releases:
                    session = item["session"]
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
        await asyncio.sleep(600)

async def task_downloader():
    logger.info("⬇️ Downloader Worker Started.")
    while True:
        # 1. Get the next pending item
        item = await col_queue.find_one({"status": "pending"})
        if not item:
            await asyncio.sleep(10)
            continue
            
        title = item["title"]
        ep = item["ep"]
        
        try:
            # 2. Mark as downloading so we don't pick it again immediately
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloading"}})
            logger.info(f"▶️ [START] Processing: {title} Ep {ep}")
            
            start_time = datetime.utcnow()

            # 3. Send Command
            dl_cmd = f'/anime {title} -e {ep} -r all'
            sent_msg = await app.send_message(TARGET_GROUP, dl_cmd)
            
            logger.info(f"⏳ Waiting for 'All done!' trigger...")

            # 4. Wait for the Koyeb Bot to finish (it edits message to "✅ All done!")
            success = await wait_for_trigger(
                trigger_text="All done", 
                start_time=start_time,
                timeout=7200 # 2 Hour timeout in case of big files
            )

            if success:
                logger.info(f"✅ [FINISH] {title} completed successfully.")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloaded"}})
                
                # --- THE COOL DOWN ---
                # This is crucial for your Koyeb server health
                logger.info("❄️ Resting for 30 minutes before next task...")
                await app.send_message(TARGET_GROUP, f"💤 Cooling down for 30 mins after **{title}**...")
                await asyncio.sleep(1800) # 30 Minutes Sleep
                
            else:
                logger.warning(f"❌ [TIMEOUT] {title} took too long.")
                # Reset to pending to try again later? Or mark failed?
                # Marking failed for now to prevent infinite loops.
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "failed_dl"}})

        except Exception as e:
            logger.error(f"Downloader Error: {e}")
            # If error, wait 1 min then retry loop
            await asyncio.sleep(60)

async def task_uploader():
    # Only marks items as "done" since Koyeb handles the actual upload
    logger.info("⬆️ Uploader Monitor Started.")
    while True:
        item = await col_queue.find_one({"status": "downloaded"})
        if not item:
            await asyncio.sleep(10)
            continue
        # Just clean up DB status
        await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "done"}})
        await asyncio.sleep(1)

async def main():
    await app.start()
    await warm_up()
    await asyncio.gather(web_server(), task_poller(), task_downloader(), task_uploader())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
