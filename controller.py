import asyncio
import logging
import os
import re
import sys
import aiohttp
from datetime import datetime, timedelta
from pyrogram import Client, filters
import motor.motor_asyncio
from aiohttp import web

# --- FIX: FORCE EVENT LOOP FOR PYTHON 3.14+ (Render Fix) ---
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

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

# Admin List (Environment Variable: ADMIN_IDS="12345, 67890")
def get_env_list(var_name):
    val = os.getenv(var_name)
    if val:
        return [int(x.strip()) for x in val.split(',') if x.strip().lstrip("-").isdigit()]
    return []

ADMIN_IDS = get_env_list("ADMIN_IDS")

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

# --- HELPERS ---
def normalize(text):
    if not text: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', text).lower()

async def is_admin(message):
    if not ADMIN_IDS: return True
    if message.from_user.id not in ADMIN_IDS:
        return False
    return True

async def wait_for_trigger(trigger_text, start_time, timeout=7200):
    logger.info(f"👀 Watching for: '{trigger_text}'...")
    loop_start = asyncio.get_event_loop().time()
    
    while True:
        if (asyncio.get_event_loop().time() - loop_start) > timeout:
            return False

        try:
            async for msg in app.get_chat_history(TARGET_GROUP, limit=5):
                if not msg.text: continue
                if trigger_text.lower() in msg.text.lower():
                    msg_time = msg.edit_date or msg.date
                    if msg_time > (start_time - timedelta(seconds=10)):
                        logger.info(f"✅ Trigger Found: {msg.text[:30]}...")
                        return True
        except Exception as e:
            logger.error(f"⚠️ Watcher Error: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(10)

async def warm_up():
    try: await app.get_chat(TARGET_GROUP)
    except: logger.error("❌ Target Group NOT found!")

# --- COMMANDS ---

@app.on_message(filters.command("start"))
async def start(client, message):
    if not await is_admin(message): return
    await message.reply("👋 Controller is Online!\n\n/list - See latest anime\n/queue - See pending jobs\n/retry_all - Reset stuck jobs")

@app.on_message(filters.command("list"))
async def list_releases(client, message):
    if not await is_admin(message): return
    
    status = await message.reply("🔍 Fetching latest releases from AnimePahe...")
    
    try:
        # Run synchronous engine code in a thread to avoid blocking
        releases = await asyncio.to_thread(engine.get_latest_releases)
        
        if not releases:
            await status.edit_text("❌ Failed to fetch releases or site is empty.")
            return

        text = "📅 **Latest Anime Releases (Page 1):**\n\n"
        for i, item in enumerate(releases, 1):
            text += f"**{i}.** `{item['title']}` - Ep {item['ep']} ({item['time']})\n"
        
        await status.edit_text(text)
        
    except Exception as e:
        logger.error(f"List Error: {e}")
        await status.edit_text(f"❌ Error: {e}")

@app.on_message(filters.command("queue"))
async def queue_status(client, message):
    if not await is_admin(message): return
    try:
        cursor = col_queue.find({"status": "pending"}).sort("found_at", 1)
        items = await cursor.to_list(length=20)
        
        if not items:
            await message.reply("✅ **Queue is empty!** No pending anime.")
            return

        text = "📋 **Pending Anime Queue:**\n\n"
        for i, item in enumerate(items, 1):
            text += f"**{i}.** {item['title']} - Ep {item['ep']} `[Pending]`\n"
        
        total = await col_queue.count_documents({"status": "pending"})
        if total > 20: text += f"\n...and {total - 20} more."

        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error fetching queue: {e}")

@app.on_message(filters.command("retry_all"))
async def retry_stuck(client, message):
    if not await is_admin(message): return
    try:
        r = await col_queue.update_many({"status": "downloading"}, {"$set": {"status": "pending"}})
        await message.reply(f"🔄 Reset {r.modified_count} stuck items to Pending.")
    except: pass

@app.on_message(filters.command("restart"))
async def restart_bot(client, message):
    if not await is_admin(message): return
    await message.reply("🔄 Restarting...")
    os.execl(sys.executable, sys.executable, *sys.argv)

# --- TASKS ---

async def task_poller():
    logger.info("📡 Polling Task Started.")
    while True:
        try:
            releases = await asyncio.to_thread(engine.get_latest_releases)
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
        item = await col_queue.find_one({"status": "pending"})
        if not item:
            await asyncio.sleep(10)
            continue
            
        title = item["title"]
        ep = item["ep"]
        
        try:
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloading"}})
            logger.info(f"▶️ [START] Processing: {title} Ep {ep}")
            
            start_time = datetime.utcnow()
            dl_cmd = f'/anime {title} -e {ep} -r all'
            await app.send_message(TARGET_GROUP, dl_cmd)
            
            logger.info(f"⏳ Waiting for 'All done!' trigger...")
            success = await wait_for_trigger("All done", start_time, timeout=7200)

            if success:
                logger.info(f"✅ [FINISH] {title} completed.")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloaded"}})
                
                logger.info("❄️ Resting for 30 minutes...")
                await app.send_message(TARGET_GROUP, f"💤 Cooling down for 30 mins after **{title}**...")
                await asyncio.sleep(1800) 
                
            else:
                logger.warning(f"❌ [TIMEOUT] {title} took too long.")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "failed_dl"}})

        except Exception as e:
            logger.error(f"Downloader Error: {e}")
            await asyncio.sleep(60)

async def task_uploader():
    logger.info("⬆️ Uploader Monitor Started.")
    while True:
        item = await col_queue.find_one({"status": "downloaded"})
        if not item:
            await asyncio.sleep(10)
            continue
        await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "done"}})
        await asyncio.sleep(1)

async def main():
    await app.start()
    await warm_up()
    await asyncio.gather(web_server(), task_poller(), task_downloader(), task_uploader())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
