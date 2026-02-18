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

# --- IMPORT ENGINE ---
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

def normalize_for_cmd(text):
    if not text: return ""
    clean = re.sub(r"['\"!?;:]", "", text)
    return clean.strip()

async def is_admin(message):
    if not ADMIN_IDS: return True
    if message.from_user.id not in ADMIN_IDS:
        return False
    return True

async def wait_for_result(start_time, timeout=7200):
    logger.info(f"👀 Watching for completion triggers...")
    loop_start = asyncio.get_event_loop().time()
    
    while True:
        if (asyncio.get_event_loop().time() - loop_start) > timeout:
            return "timeout"

        try:
            # Check last 20 messages (Increased range to ignore spam from other bots)
            async for msg in app.get_chat_history(TARGET_GROUP, limit=20):
                if not msg.text: continue
                
                msg_time = msg.edit_date or msg.date
                if msg_time < (start_time - timedelta(seconds=10)):
                    continue

                text = msg.text.lower()

                # 1. SUCCESS TRIGGER
                if "all done" in text:
                    logger.info(f"✅ Detect: Success")
                    return "success"
                
                # 2. PARTIAL FAILURE TRIGGER (Still counts as 'done' for queue purposes)
                if "job finished" in text or "successful" in text:
                    logger.info(f"⚠️ Detect: Partial Success")
                    return "success" # Treat partials as success to stop retrying loops

                # 3. CRITICAL FAILURE TRIGGER
                if "task finished" in text or "failed to download" in text:
                    logger.info(f"❌ Detect: Failure")
                    return "failed"

        except Exception as e:
            logger.error(f"⚠️ Watcher Error: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(10)

async def warm_up():
    logger.info("🔥 Checking Target Group access...")
    try:
        chat = await app.get_chat(TARGET_GROUP)
        logger.info(f"✅ Target Group Found: {chat.title}")
    except Exception as e:
        logger.warning(f"⚠️ Target Group not cached yet. ({e})")

# --- COMMANDS ---

@app.on_message(filters.command("start"))
async def start(client, message):
    if not await is_admin(message): return
    await message.reply("👋 Controller is Online!\n\n/list - Latest releases\n/queue - View status\n/retry_all - Retry failed items\n/clear_queue - Delete pending items")

@app.on_message(filters.command("list"))
async def list_releases(client, message):
    if not await is_admin(message): return
    status = await message.reply("🔍 Fetching releases...")
    try:
        releases = await asyncio.to_thread(engine.get_latest_releases)
        if not releases:
            await status.edit_text("❌ Failed to fetch releases.")
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
        # Show PENDING (Priority) first, then FAILED (Waiting manual retry)
        pending_count = await col_queue.count_documents({"status": "pending"})
        failed_count = await col_queue.count_documents({"status": "failed_dl"})
        
        cursor = col_queue.find({"status": {"$in": ["pending", "failed_dl"]}}).sort([("status", -1), ("found_at", 1)])
        items = await cursor.to_list(length=30)
        
        if not items:
            await message.reply("✅ **Queue is empty!**")
            return

        text = f"📋 **Queue Status**\n⏳ Pending: `{pending_count}` | ⚠️ Failed: `{failed_count}`\n\n"
        
        for i, item in enumerate(items, 1):
            status_icon = "⏳" if item['status'] == "pending" else "⚠️ Failed"
            text += f"**{i}.** {item['title']} - Ep {item['ep']} `{status_icon}`\n"
        
        total = pending_count + failed_count
        if total > 30: text += f"\n...and {total - 30} more."

        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("retry_all"))
async def retry_stuck(client, message):
    if not await is_admin(message): return
    try:
        # Resets FAILED items to PENDING so they get picked up
        r = await col_queue.update_many(
            {"status": "failed_dl"}, 
            {"$set": {"status": "pending"}}
        )
        await message.reply(f"🔄 Queued {r.modified_count} failed items for retry.")
    except: pass

@app.on_message(filters.command("clear_queue"))
async def clear_queue(client, message):
    if not await is_admin(message): return
    try:
        # Deletes ALL pending or failed items
        r = await col_queue.delete_many({"status": {"$in": ["pending", "failed_dl"]}})
        await message.reply(f"🗑️ Deleted {r.deleted_count} items from queue.")
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
                    # If not exists in DB, add it
                    if not await col_queue.find_one({"_id": session}):
                        entry = {
                            "_id": session,
                            "title": item["title"],
                            "ep": item["ep"],
                            "time": item["time"],
                            "found_at": datetime.utcnow(),
                            "status": "pending" # New items are always PENDING
                        }
                        await col_queue.insert_one(entry)
                        logger.info(f"🆕 Queued: {item['title']} - Ep {item['ep']}")
        except Exception as e:
            logger.error(f"Polling Error: {e}")
        await asyncio.sleep(600)

async def task_downloader():
    logger.info("⬇️ Downloader Worker Started.")
    while True:
        # 1. PRIORITY: Pick "pending" items (Fresh releases or manually retried)
        # We ignore "failed_dl" items until user types /retry_all
        item = await col_queue.find_one({"status": "pending"}, sort=[("found_at", 1)])
        
        if not item:
            await asyncio.sleep(10)
            continue
            
        title = item["title"]
        ep = item["ep"]
        
        try:
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloading"}})
            
            safe_title = normalize_for_cmd(title)
            
            logger.info(f"▶️ [START] Processing: {safe_title} Ep {ep}")
            start_time = datetime.utcnow()
            
            dl_cmd = f'/anime {safe_title} -e {ep} -r all'
            await app.send_message(TARGET_GROUP, dl_cmd)
            
            result = await wait_for_result(start_time, timeout=7200)

            if result == "success":
                logger.info(f"✅ [FINISH] {title} completed.")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloaded"}})
                
                logger.info("❄️ Success. Cooling down for 30 minutes...")
                await app.send_message(TARGET_GROUP, f"💤 Success! Cooling down 30m after **{safe_title}**...")
                await asyncio.sleep(1800) 

            else: 
                # FAILED or TIMEOUT
                logger.warning(f"❌ [FAILED] {title}")
                # Mark as 'failed_dl' and DO NOT RETRY automatically.
                # User must use /retry_all to try again.
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "failed_dl"}})
                
                # Short cool down on failure to protect server
                await asyncio.sleep(120)

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
